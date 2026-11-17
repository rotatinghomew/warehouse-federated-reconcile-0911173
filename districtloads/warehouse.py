import json

from .exchange import (BatchConflict, ChecksumMismatch, DuplicateKeyConflict,
                       SubmissionError, definition_fingerprint_of, normalize,
                       scope_covers, shard_checksum)
from .storage import atomic, connect, records

IMPLICIT_SHARD = '__whole__'

DDL = """
CREATE TABLE IF NOT EXISTS batches(
    name TEXT PRIMARY KEY,
    district TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    mode TEXT NOT NULL,
    declared_rows INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    definition_checksum TEXT NOT NULL DEFAULT '',
    scope TEXT NOT NULL DEFAULT '{"kind":"district"}',
    complete INTEGER NOT NULL DEFAULT 1,
    replaces_batch TEXT,
    manifest TEXT,
    status TEXT NOT NULL DEFAULT 'promoted',
    file_name TEXT,
    raw TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS source_sequence ON batches(district,sequence);
CREATE TABLE IF NOT EXISTS shards(
    batch TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    declared_rows INTEGER NOT NULL,
    declared_checksum TEXT NOT NULL,
    observed_checksum TEXT,
    received_rows INTEGER,
    file_name TEXT,
    status TEXT NOT NULL DEFAULT 'missing',
    raw TEXT,
    PRIMARY KEY(batch,shard_id)
);
CREATE TABLE IF NOT EXISTS landed_rows(
    batch TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    school TEXT NOT NULL,
    day TEXT,
    people INTEGER,
    op TEXT NOT NULL,
    PRIMARY KEY(batch,shard_id,ordinal)
);
CREATE UNIQUE INDEX IF NOT EXISTS landed_batch_key ON landed_rows(batch,event_id);
CREATE TABLE IF NOT EXISTS facts(
    district TEXT NOT NULL,
    event_id TEXT NOT NULL,
    school TEXT NOT NULL,
    day TEXT,
    people INTEGER,
    batch TEXT NOT NULL,
    sequence INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0,
    deleted_batch TEXT,
    deleted_reason TEXT,
    PRIMARY KEY(district,event_id)
);
CREATE TABLE IF NOT EXISTS coverage(
    district TEXT NOT NULL,
    school TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    batch TEXT NOT NULL,
    PRIMARY KEY(district,school)
);
CREATE TABLE IF NOT EXISTS district_coverage(
    district TEXT PRIMARY KEY,
    sequence INTEGER NOT NULL,
    batch TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS source_heads(district TEXT PRIMARY KEY,sequence INTEGER);
CREATE TABLE IF NOT EXISTS publications(name TEXT PRIMARY KEY,payload TEXT);
CREATE TABLE IF NOT EXISTS receipts(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    file_name TEXT,
    checksum TEXT NOT NULL,
    rows INTEGER NOT NULL,
    outcome TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS adjustments(
    batch TEXT NOT NULL,
    district TEXT NOT NULL,
    event_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    fact_batch TEXT,
    fact_sequence INTEGER,
    detail TEXT,
    UNIQUE(batch,district,event_id,kind)
);
"""


def _columns(database, table):
    return {row[1] for row in database.execute(f'PRAGMA table_info({table})')}


def migrate(database):
    """Bring a database created by the previous single-file schema forward."""
    facts = _columns(database, 'facts')
    additions = {
        'sequence': 'INTEGER NOT NULL DEFAULT 0',
        'deleted': 'INTEGER NOT NULL DEFAULT 0',
        'deleted_batch': 'TEXT',
        'deleted_reason': 'TEXT',
    }
    for name, declaration in additions.items():
        if name not in facts:
            database.execute(f'ALTER TABLE facts ADD COLUMN {name} {declaration}')
    database.execute('''UPDATE facts SET sequence=COALESCE(
        (SELECT sequence FROM batches WHERE batches.name=facts.batch),0)
        WHERE sequence=0''')

    batches = _columns(database, 'batches')
    additions = {
        'definition_checksum': "TEXT NOT NULL DEFAULT ''",
        'scope': "TEXT NOT NULL DEFAULT '{\"kind\":\"district\"}'",
        'complete': 'INTEGER NOT NULL DEFAULT 1',
        'replaces_batch': 'TEXT',
        'manifest': 'TEXT',
        'status': "TEXT NOT NULL DEFAULT 'promoted'",
        'file_name': 'TEXT',
    }
    for name, declaration in additions.items():
        if name not in batches:
            database.execute(f'ALTER TABLE batches ADD COLUMN {name} {declaration}')


def _scope_json(scope):
    return json.dumps(scope, ensure_ascii=False, sort_keys=True)


class ExchangeWarehouse:
    def __init__(self, root):
        self.db = connect(root, DDL)
        migrate(self.db)

    def close(self):
        self.db.close()

    # -- public API -----------------------------------------------------

    def apply(self, document):
        """Accept one submission file.

        Returns True when the submission advanced warehouse state and False for
        an exact replay of an already successful submission.  Identity is never
        derived from the file name.
        """
        submission = normalize(document)
        with atomic(self.db):
            existing = self._batch(submission['batch'])
            if existing is None:
                if submission['shard_id'] is not None and submission['manifest'] is None:
                    raise SubmissionError("补交传输片必须指向已存在的报送批次")
                result = self._accept_new(submission)
            else:
                result = self._accept_existing(submission, existing)
            if result['receipt']:
                self._record_receipt(submission, result['receipt'])
        if result['error']:
            raise result['error']
        return result['changed']

    def detail(self):
        return records(
            self.db,
            '''SELECT district,event_id,school,day,people,batch,sequence
               FROM facts WHERE deleted=0 ORDER BY district,event_id''')

    def tombstones(self):
        return records(
            self.db,
            '''SELECT district,event_id,school,batch,sequence,deleted_batch,
               deleted_reason FROM facts WHERE deleted=1
               ORDER BY district,event_id''')

    def shards(self):
        return records(self.db, 'SELECT * FROM shards ORDER BY batch,shard_id')

    def batches(self):
        return records(self.db, 'SELECT * FROM batches ORDER BY district,sequence')

    def pending(self):
        return records(
            self.db,
            "SELECT * FROM batches WHERE status!='promoted' ORDER BY district,sequence")

    def adjustments(self):
        return records(
            self.db,
            'SELECT * FROM adjustments ORDER BY district,batch,event_id,kind')

    def receipts(self):
        return records(self.db, 'SELECT * FROM receipts ORDER BY id')

    # -- acceptance -----------------------------------------------------

    def _batch(self, name):
        row = self.db.execute('SELECT * FROM batches WHERE name=?', (name,)).fetchone()
        return dict(row) if row else None

    def _stored_scope(self, batch):
        return json.loads(batch['scope'])

    def _result(self, changed=False, error=None, receipt=None):
        return {'changed': changed, 'error': error, 'receipt': receipt}

    def _accept_new(self, submission):
        self._check_sequence(submission)
        self._check_correction(submission)
        self.db.execute(
            '''INSERT INTO batches(name,district,sequence,mode,declared_rows,checksum,
               definition_checksum,scope,complete,replaces_batch,manifest,status,file_name,raw)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (submission['batch'], submission['district'], submission['sequence'],
             submission['mode'], submission['declared_rows'],
             submission['content_checksum'], submission['definition_checksum'],
             _scope_json(submission['scope']), 1 if submission['complete'] else 0,
             submission['replaces_batch'],
             json.dumps(submission['manifest'], ensure_ascii=False, sort_keys=True)
             if submission['manifest'] else None,
             'pending', submission['file_name'],
             json.dumps(submission['document'], ensure_ascii=False, sort_keys=True)))
        if submission['manifest'] is not None:
            for entry in submission['manifest']:
                self.db.execute(
                    '''INSERT INTO shards(batch,shard_id,declared_rows,declared_checksum,status)
                       VALUES(?,?,?,?,'missing')''',
                    (submission['batch'], entry['shard_id'], entry['declared_rows'],
                     entry['checksum']))
        else:
            self.db.execute(
                '''INSERT INTO shards(batch,shard_id,declared_rows,declared_checksum,status)
                   VALUES(?,?,?,?,'missing')''',
                (submission['batch'], IMPLICIT_SHARD, submission['declared_rows'],
                 submission['content_checksum']))
        result = self._accept_shard(submission)
        result['changed'] = True
        return result

    def _accept_existing(self, submission, existing):
        if submission['manifest'] is None and existing['manifest'] is not None:
            submission = self._resolve_backfill(submission, existing)
        if (existing['definition_checksum']
                and existing['definition_checksum'] != submission['definition_checksum']):
            return self._result(error=BatchConflict(
                "报送批次定义冲突：批次身份、范围、模式或清单被改写"))

        if existing['manifest'] is None:
            if existing['checksum'] != submission['content_checksum']:
                return self._result(error=BatchConflict(
                    "报送批次内容冲突：同一批次收到不同内容"))
            if bool(existing['complete']) or not submission['complete']:
                return self._result()
            self.db.execute('UPDATE batches SET complete=1 WHERE name=?',
                            (submission['batch'],))
            self._promote(self._batch(submission['batch']), force=True)
            return self._result(changed=True)

        return self._accept_shard(submission)

    def _resolve_backfill(self, submission, existing):
        """Attach a shard-only file to the batch it declares it belongs to.

        The file carries no manifest of its own, so identity, scope and
        completion come from the original batch statement.  A backfill cannot
        introduce a new shard, change a scope, or close a roster by itself.
        """
        if 'complete' in submission['document']:
            raise BatchConflict("补交传输片不能单独声明清册完整")
        if existing['district'] != submission['district']:
            raise BatchConflict("补交传输片与报送批次所属地区不一致")
        if existing['sequence'] != submission['sequence']:
            raise BatchConflict("补交传输片与原批次序号不一致")
        if existing['mode'] != submission['mode']:
            raise BatchConflict("补交传输片与原批次模式不一致")
        if ('scope' in submission['document']
                and self._stored_scope(existing) != submission['scope']):
            raise BatchConflict("补交传输片与原批次覆盖范围不一致")
        if (submission['replaces_batch'] is not None
                and submission['replaces_batch'] != existing['replaces_batch']):
            raise BatchConflict("补交传输片与原批次替代关系不一致")

        manifest = json.loads(existing['manifest'])
        resolved = dict(submission)
        resolved['scope'] = self._stored_scope(existing)
        resolved['manifest'] = manifest
        resolved['declared_rows'] = existing['declared_rows']
        resolved['replaces_batch'] = existing['replaces_batch']
        # A backfill never closes an open roster; it only supplies its bytes.
        resolved['complete'] = bool(existing['complete'])
        resolved['definition_checksum'] = definition_fingerprint_of(resolved)
        resolved['content_checksum'] = shard_checksum(
            resolved['batch'], resolved['district'], resolved['sequence'],
            resolved['shard_id'], resolved['rows'])
        return resolved

    def _accept_shard(self, submission):
        submission = dict(submission,
                          shard_id=submission['shard_id'] or IMPLICIT_SHARD)
        shard = self.db.execute(
            'SELECT * FROM shards WHERE batch=? AND shard_id=?',
            (submission['batch'], submission['shard_id'])).fetchone()
        if shard is None:
            return self._result(error=SubmissionError("本次传输片不在报送清单内"))
        shard = dict(shard)

        if shard['declared_rows'] != len(submission['rows']):
            self._mark_shard(submission, 'mismatch')
            return self._result(
                error=ChecksumMismatch("传输片行数与来源清单声明不一致"),
                receipt='row_count_mismatch')

        observed = submission['content_checksum']
        if observed != shard['declared_checksum']:
            self._mark_shard(submission, 'mismatch', observed)
            return self._result(
                error=ChecksumMismatch("传输片摘要与来源清单声明不一致"),
                receipt='checksum_mismatch')

        if shard['status'] == 'received':
            if shard['observed_checksum'] != observed:
                return self._result(error=BatchConflict(
                    "同一传输片收到不同内容，不能按文件名视为同一次成功"))
            if submission['complete'] and not self._complete(submission['batch']):
                self.db.execute('UPDATE batches SET complete=1 WHERE name=?',
                                (submission['batch'],))
                self._promote(self._batch(submission['batch']), force=True)
                return self._result(changed=True)
            return self._result()

        conflict = self._stage_shard_rows(submission)
        if conflict is not None:
            self._mark_shard(submission, 'conflict', observed)
            return self._result(error=DuplicateKeyConflict(conflict),
                                receipt='key_conflict')

        self._mark_shard(submission, 'received', observed)
        if submission['complete'] and not self._complete(submission['batch']):
            self.db.execute('UPDATE batches SET complete=1 WHERE name=?',
                            (submission['batch'],))
        self._promote(self._batch(submission['batch']))
        return self._result(changed=True, receipt='received')

    def _complete(self, batch_name):
        batch = self._batch(batch_name)
        return bool(batch['complete'])

    def _mark_shard(self, submission, status, observed=None):
        self.db.execute(
            '''UPDATE shards SET status=?,observed_checksum=?,received_rows=?,
               file_name=?,raw=? WHERE batch=? AND shard_id=?''',
            (status, observed, len(submission['rows']), submission['file_name'],
             json.dumps(submission['document'], ensure_ascii=False, sort_keys=True),
             submission['batch'], submission['shard_id']))

    def _stage_shard_rows(self, submission):
        for row in submission['rows']:
            other = self.db.execute(
                '''SELECT shard_id FROM landed_rows
                   WHERE batch=? AND event_id=? AND shard_id!=?''',
                (submission['batch'], row['event_id'], submission['shard_id'])).fetchone()
            if other:
                return f"业务键{row['event_id']}同时出现在传输片{other[0]}与本片"
        for ordinal, row in enumerate(submission['rows']):
            self.db.execute(
                '''INSERT INTO landed_rows(batch,shard_id,ordinal,event_id,school,day,people,op)
                   VALUES(?,?,?,?,?,?,?,?)''',
                (submission['batch'], submission['shard_id'], ordinal,
                 row['event_id'], row['school'], row['day'], row['people'], row['op']))
        return None

    def _record_receipt(self, submission, outcome):
        self.db.execute(
            '''INSERT INTO receipts(batch,shard_id,file_name,checksum,rows,outcome)
               VALUES(?,?,?,?,?,?)''',
            (submission['batch'], submission['shard_id'] or IMPLICIT_SHARD,
             submission.get('file_name'), submission['content_checksum'],
             len(submission['rows']), outcome))

    def _check_sequence(self, submission):
        row = self.db.execute(
            'SELECT MAX(sequence) FROM batches WHERE district=?',
            (submission['district'],)).fetchone()
        expected = (row[0] or 0) + 1
        if submission['sequence'] != expected:
            raise ValueError("源端序号不连续")

    def _check_correction(self, submission):
        if submission['mode'] != 'correction':
            return
        if not submission['replaces_batch']:
            raise SubmissionError("更正批次必须声明被更正批次")
        target = self._batch(submission['replaces_batch'])
        if target is None:
            raise SubmissionError("被更正批次不存在")
        if target['district'] != submission['district']:
            raise SubmissionError("被更正批次不属于同一地区")
        target_scope = self._stored_scope(target)
        if target_scope['kind'] == 'schools':
            outside = [school for school in submission['scope']['schools']
                       if school not in set(target_scope['schools'])]
            if outside:
                raise SubmissionError("更正范围超出被更正批次声明的范围")
        # A correction replaces exactly the target batch's scope, never more.
        for row in submission['rows']:
            if not scope_covers(target_scope, row['school']):
                raise SubmissionError("更正行超出被更正批次声明的范围")

    # -- promotion ------------------------------------------------------

    def _promote(self, batch, force=False):
        if batch is None:
            raise SubmissionError("报送批次不存在")
        if batch['status'] == 'promoted' and not force:
            return False
        if batch['manifest'] is not None:
            shards = records(self.db, 'SELECT * FROM shards WHERE batch=?',
                             (batch['name'],))
            if any(shard['status'] != 'received' for shard in shards):
                self.db.execute("UPDATE batches SET status='pending' WHERE name=?",
                                (batch['name'],))
                return False
        rows = records(
            self.db,
            'SELECT * FROM landed_rows WHERE batch=? ORDER BY shard_id,ordinal',
            (batch['name'],))
        scope = json.loads(batch['scope'])
        # Adjustments describe the decisions of the final promotion; re-running
        # a promotion (for example when an open roster is explicitly closed)
        # must not leave stale unresolved ranges behind.
        self.db.execute('DELETE FROM adjustments WHERE batch=?', (batch['name'],))
        for row in rows:
            if row['op'] == 'delete':
                self._delete_fact(batch, row)
            else:
                self._upsert_fact(batch, row)
        if batch['complete'] and batch['mode'] in ('full', 'correction'):
            self._delete_absent(batch, scope, {row['event_id'] for row in rows})
            self._advance_coverage(batch, scope, rows)
        elif batch['mode'] in ('full', 'correction'):
            self._record_unresolved_absence(batch, scope, {row['event_id'] for row in rows})
        self.db.execute("UPDATE batches SET status='promoted' WHERE name=?",
                        (batch['name'],))
        self.db.execute(
            '''INSERT INTO source_heads(district,sequence) VALUES(?,?)
               ON CONFLICT(district) DO UPDATE SET sequence=MAX(sequence,excluded.sequence)''',
            (batch['district'], batch['sequence']))
        return True

    def _covered(self, batch, school):
        """Highest sequence at which a closed roster confirmed this school.

        A district-wide closed roster confirms every school, so it counts even
        when the school contributed no rows (an empty or narrowed roster is
        still a statement that no other keys survive there).
        """
        school_watermark = self.db.execute(
            'SELECT sequence FROM coverage WHERE district=? AND school=?',
            (batch['district'], school)).fetchone()
        district_watermark = self.db.execute(
            'SELECT sequence FROM district_coverage WHERE district=?',
            (batch['district'],)).fetchone()
        return max(school_watermark[0] if school_watermark else 0,
                   district_watermark[0] if district_watermark else 0)

    def _upsert_fact(self, batch, row):
        fact = self.db.execute(
            'SELECT * FROM facts WHERE district=? AND event_id=?',
            (batch['district'], row['event_id'])).fetchone()
        if fact is None:
            if self._covered(batch, row['school']) > batch['sequence']:
                self._adjust(batch, row, 'skipped_stale_upsert', None,
                             '较晚完整清册已确认该学校当前业务键集合，不再补入')
                return
            self.db.execute(
                '''INSERT INTO facts(district,event_id,school,day,people,batch,sequence,deleted)
                   VALUES(?,?,?,?,?,?,?,0)''',
                (batch['district'], row['event_id'], row['school'], row['day'],
                 row['people'], batch['name'], batch['sequence']))
            return
        fact = dict(fact)
        if fact['sequence'] > batch['sequence']:
            self._adjust(batch, row, 'skipped_stale_upsert', fact,
                         '较晚批次已确认该业务键，较早批次不能覆盖')
            return
        self.db.execute(
            '''UPDATE facts SET school=?,day=?,people=?,batch=?,sequence=?,
               deleted=0,deleted_batch=NULL,deleted_reason=NULL
               WHERE district=? AND event_id=?''',
            (row['school'], row['day'], row['people'], batch['name'],
             batch['sequence'], batch['district'], row['event_id']))

    def _delete_fact(self, batch, row):
        fact = self.db.execute(
            'SELECT * FROM facts WHERE district=? AND event_id=?',
            (batch['district'], row['event_id'])).fetchone()
        fact = dict(fact) if fact else None
        if fact is not None and fact['sequence'] > batch['sequence']:
            self._adjust(batch, row, 'skipped_stale_delete', fact,
                         '较晚批次已确认该业务键，不能被较早的删除清掉')
            return
        school = row['school'] or (fact and fact['school']) or ''
        day = row['day'] if row['day'] is not None else (fact and fact['day'])
        people = row['people'] if row['people'] is not None else (fact and fact['people'])
        self.db.execute(
            '''INSERT INTO facts(district,event_id,school,day,people,batch,sequence,
               deleted,deleted_batch,deleted_reason)
               VALUES(?,?,?,?,?,?,?,1,?,'explicit')
               ON CONFLICT(district,event_id) DO UPDATE SET
                 school=excluded.school,day=excluded.day,people=excluded.people,
                 batch=excluded.batch,sequence=excluded.sequence,
                 deleted=1,deleted_batch=excluded.deleted_batch,
                 deleted_reason='explicit' ''',
            (batch['district'], row['event_id'], school, day, people,
             batch['name'], batch['sequence'], batch['name']))

    def _delete_absent(self, batch, scope, present_keys):
        candidates = records(
            self.db,
            'SELECT * FROM facts WHERE district=? AND deleted=0 ORDER BY event_id',
            (batch['district'],))
        for fact in candidates:
            if not scope_covers(scope, fact['school']):
                # The school was never part of this submission's scope.
                continue
            if fact['event_id'] in present_keys:
                continue
            if fact['sequence'] > batch['sequence']:
                self._adjust(batch, fact, 'retained_later_key', fact,
                             '不在本次清册内，但已有较晚批次确认，保留')
                continue
            if (batch['mode'] == 'correction'
                    and fact['batch'] != batch['replaces_batch']):
                # A correction replaces only the batch it names; keys from
                # other batches need an explicit delete.
                self._adjust(batch, fact, 'retained_other_batch', fact,
                             '更正批次只替代其声明的原批次，其他批次业务键保留')
                continue
            self.db.execute(
                '''UPDATE facts SET deleted=1,deleted_batch=?,
                   deleted_reason='roster_omission',batch=?,sequence=?
                   WHERE district=? AND event_id=?''',
                (batch['name'], batch['name'], batch['sequence'],
                 batch['district'], fact['event_id']))

    def _record_unresolved_absence(self, batch, scope, present_keys):
        candidates = records(
            self.db,
            'SELECT * FROM facts WHERE district=? AND deleted=0 ORDER BY event_id',
            (batch['district'],))
        for fact in candidates:
            if not scope_covers(scope, fact['school']):
                continue
            if fact['event_id'] in present_keys:
                continue
            self._adjust(batch, fact, 'unresolved_missing_key', fact,
                         '清册尚未声明完整，缺少的业务键不能推导为删除')

    def _advance_coverage(self, batch, scope, rows):
        schools = {row['school'] for row in rows}
        if scope['kind'] == 'schools':
            # A closed scoped roster confirms those schools even when a school
            # contributed no rows (it has no surviving keys).
            schools |= set(scope['schools'])
        if scope['kind'] == 'district':
            # An empty closed district roster is still a declaration that every
            # school in the district has no surviving keys, so the watermark
            # must advance even without rows.
            schools |= {fact['school'] for fact in records(
                self.db,
                'SELECT DISTINCT school FROM facts WHERE district=?',
                (batch['district'],))}
            schools |= {row[0] for row in self.db.execute(
                'SELECT school FROM coverage WHERE district=?',
                (batch['district'],))}
            self.db.execute(
                '''INSERT INTO district_coverage(district,sequence,batch) VALUES(?,?,?)
                   ON CONFLICT(district) DO UPDATE SET
                     sequence=MAX(sequence,excluded.sequence),
                     batch=CASE WHEN excluded.sequence>=sequence
                                THEN excluded.batch ELSE batch END''',
                (batch['district'], batch['sequence'], batch['name']))
        for school in schools:
            self.db.execute(
                '''INSERT INTO coverage(district,school,sequence,batch) VALUES(?,?,?,?)
                   ON CONFLICT(district,school) DO UPDATE SET
                     sequence=MAX(sequence,excluded.sequence),
                     batch=CASE WHEN excluded.sequence>=sequence
                                THEN excluded.batch ELSE batch END''',
                (batch['district'], school, batch['sequence'], batch['name']))

    def _adjust(self, batch, row, kind, fact, detail):
        self.db.execute(
            '''INSERT OR IGNORE INTO adjustments
               (batch,district,event_id,kind,fact_batch,fact_sequence,detail)
               VALUES(?,?,?,?,?,?,?)''',
            (batch['name'], batch['district'], row['event_id'], kind,
             fact.get('batch') if fact else None,
             fact.get('sequence') if fact else None, detail))
