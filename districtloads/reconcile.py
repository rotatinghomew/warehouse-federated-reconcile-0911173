import json

from .exchange import scope_covers
from .storage import atomic, records


class IncompleteError(ValueError):
    """A requested release lacks the completeness conditions it claims."""

    def __init__(self, unresolved):
        self.unresolved = unresolved
        super().__init__("完整性条件未满足，不能发布该口径")


def _summary(facts):
    grouped = {}
    for fact in facts:
        key = (fact['district'], fact['school'])
        bucket = grouped.setdefault(key, {'district': fact['district'],
                                          'school': fact['school'],
                                          'events': 0, 'people': 0})
        bucket['events'] += 1
        bucket['people'] += fact['people'] or 0
    return [grouped[key] for key in sorted(grouped)]


def _in_scope(scope, school):
    return scope is None or scope_covers(scope, school)


def _union(scopes):
    """Union of scopes; ``None`` when the union contains no school."""
    whole = any(scope is not None and scope.get('kind') == 'district'
                for scope in scopes)
    if whole:
        return {'kind': 'district'}
    schools = set()
    for scope in scopes:
        if scope is not None:
            schools |= set(scope.get('schools', ()))
    return {'kind': 'schools', 'schools': sorted(schools)} if schools else None


def _scope_contains(outer, inner):
    if outer is None or inner is None:
        return False
    if outer.get('kind') == 'district':
        return True
    if inner.get('kind') == 'district':
        return False
    return set(inner.get('schools', ())) <= set(outer.get('schools', ()))


def _confirmed(batch):
    """A roster whose declared closed content has been fully received."""
    return (batch['status'] == 'promoted'
            and bool(batch['complete'])
            and batch['mode'] in ('full', 'correction'))


def _superseded_batches(batches):
    """Batches whose gaps a later complete roster already replaced.

    A later complete roster covering the same scope means the earlier gap no
    longer blocks a release: either the later statement contains the missing
    keys or it explicitly confirms their absence.
    """
    superseded = set()
    for batch in batches:
        for later in batches:
            if later['sequence'] <= batch['sequence']:
                continue
            if not _confirmed(later):
                continue
            if _scope_contains(json.loads(later['scope']),
                               json.loads(batch['scope'])):
                superseded.add(batch['name'])
                break
    return superseded


def _batch_report(warehouse, batch, shards, adjustments):
    own_shards = [shard for shard in shards if shard['batch'] == batch['name']]
    by_status = {}
    for shard in own_shards:
        by_status.setdefault(shard['status'], []).append(shard)

    landed = warehouse.db.execute(
        'SELECT COUNT(*) FROM landed_rows WHERE batch=?',
        (batch['name'],)).fetchone()[0]
    upserts = warehouse.db.execute(
        "SELECT COUNT(*) FROM landed_rows WHERE batch=? AND op='upsert'",
        (batch['name'],)).fetchone()[0]
    deletes = landed - upserts
    facts_from_batch = warehouse.db.execute(
        'SELECT COUNT(*) FROM facts WHERE batch=? AND deleted=0',
        (batch['name'],)).fetchone()[0]
    tombstones = warehouse.db.execute(
        "SELECT COUNT(*) FROM facts WHERE deleted_batch=? AND deleted=1",
        (batch['name'],)).fetchone()[0]
    own_adjustments = [item for item in adjustments if item['batch'] == batch['name']]

    missing_rows = sum(shard['declared_rows'] for shard in own_shards
                       if shard['status'] == 'missing')
    received_rows = sum(shard['received_rows'] or 0 for shard in own_shards
                        if shard['status'] == 'received')

    differences = []
    for shard in own_shards:
        if shard['status'] == 'missing':
            differences.append({
                'kind': 'missing_shard',
                'shard_id': shard['shard_id'],
                'expected_rows': shard['declared_rows'],
                'detail': '来源清单声明了该传输片，但仓层尚未收到',
            })
        elif shard['status'] == 'mismatch':
            differences.append({
                'kind': 'checksum_mismatch',
                'shard_id': shard['shard_id'],
                'declared_checksum': shard['declared_checksum'],
                'observed_checksum': shard['observed_checksum'],
                'declared_rows': shard['declared_rows'],
                'received_rows': shard['received_rows'],
                'detail': '收到的传输片与来源清单的摘要或行数不符',
            })
        elif shard['status'] == 'conflict':
            differences.append({
                'kind': 'key_conflict',
                'shard_id': shard['shard_id'],
                'detail': '同一批次的不同传输片携带了相同业务键',
            })

    for item in own_adjustments:
        kind, text = {
            'skipped_stale_upsert': ('stale_upsert', '较早批次的更新因较晚批次已确认而未覆盖'),
            'skipped_stale_delete': ('stale_delete', '较早批次的删除因较晚批次已确认而未生效'),
            'retained_later_key': ('business_difference', '清册未列出但已有较晚批次确认，保留'),
            'retained_other_batch': ('business_difference', '更正批次未声明的其他批次业务键保留'),
            'unresolved_missing_key': ('missing_key', '清册尚未声明完整，缺少的业务键不能推导为删除'),
        }.get(item['kind'], ('business_difference', item['kind']))
        differences.append({
            'kind': kind,
            'event_id': item['event_id'],
            'reason': item['kind'],
            'fact_batch': item['fact_batch'],
            'fact_sequence': item['fact_sequence'],
            'detail': text,
        })

    return {
        'batch': batch['name'],
        'district': batch['district'],
        'sequence': batch['sequence'],
        'mode': batch['mode'],
        'scope': json.loads(batch['scope']),
        'complete': bool(batch['complete']),
        'status': batch['status'],
        'replaces_batch': batch['replaces_batch'],
        'declared_rows': batch['declared_rows'],
        'expected_rows_from_shards': sum(shard['declared_rows'] for shard in own_shards),
        'received_rows': received_rows,
        'landed_rows': landed,
        'missing_rows': missing_rows,
        'shards': {
            'declared': len(own_shards),
            'received': len(by_status.get('received', [])),
            'missing': [shard['shard_id'] for shard in by_status.get('missing', [])],
            'mismatch': [shard['shard_id'] for shard in by_status.get('mismatch', [])],
            'conflict': [shard['shard_id'] for shard in by_status.get('conflict', [])],
        },
        'effects': {
            'upserts': upserts,
            'deletes': deletes,
            'net_change': upserts - deletes,
            'facts_from_batch': facts_from_batch,
            'tombstones_from_batch': tombstones,
        },
        'differences': differences,
    }


def reconcile(warehouse, district=None):
    """Join source statements, landed shards, warehouse facts and summary.

    Every difference is classified so an operator can distinguish a transport
    gap, a validation failure and a real business difference, and can see
    whether an affected key was simply outside the declared scope.
    """
    shards = warehouse.shards()
    adjustments = warehouse.adjustments()
    batches = warehouse.batches()
    facts = warehouse.detail()
    districts = sorted({batch['district'] for batch in batches}
                       | {fact['district'] for fact in facts})
    if district is not None:
        districts = [name for name in districts if name == district]

    reports = []
    for name in districts:
        own_batches = [batch for batch in batches if batch['district'] == name]
        own_facts = [fact for fact in facts if fact['district'] == name]
        superseded = _superseded_batches(own_batches)
        # Only a roster declares a scope.  A delta changes keys inside the
        # range it was given, but its default district scope is not a claim
        # that every school has been enumerated.
        rosters = [batch for batch in own_batches
                   if batch['mode'] in ('full', 'correction')]
        declared_scope = _union([json.loads(batch['scope']) for batch in rosters])
        confirmed = [batch for batch in rosters if _confirmed(batch)]
        confirmed_scope = _union([json.loads(batch['scope']) for batch in confirmed])
        batch_reports = [_batch_report(warehouse, batch, shards, adjustments)
                         for batch in own_batches]

        blocking = []
        deferred = []
        for report in batch_reports:
            gap = (report['shards']['missing'] or report['shards']['mismatch']
                   or report['shards']['conflict']
                   or report['status'] != 'promoted')
            if report['batch'] in superseded:
                if gap:
                    deferred.append({'kind': 'superseded_gap',
                                     'batch': report['batch'],
                                     'scope': 'transport',
                                     'detail': '较晚的完整清册已取代该批次的缺口'})
                continue
            for shard_id in report['shards']['missing']:
                blocking.append({'kind': 'missing_shard', 'batch': report['batch'],
                                 'shard_id': shard_id, 'scope': 'transport'})
            for shard_id in report['shards']['mismatch']:
                blocking.append({'kind': 'checksum_mismatch', 'batch': report['batch'],
                                 'shard_id': shard_id, 'scope': 'validation'})
            for shard_id in report['shards']['conflict']:
                blocking.append({'kind': 'key_conflict', 'batch': report['batch'],
                                 'shard_id': shard_id, 'scope': 'validation'})
            if report['status'] != 'promoted':
                blocking.append({'kind': 'pending_batch', 'batch': report['batch'],
                                 'scope': 'transport'})
            if report['mode'] in ('full', 'correction') and not report['complete']:
                blocking.append({'kind': 'open_roster', 'batch': report['batch'],
                                 'scope': 'completeness',
                                 'scope_of_batch': report['scope'],
                                 'detail': '清册尚未声明完整，其范围内缺失不能推导为删除'})

        if confirmed_scope is None:
            blocking.append({'kind': 'no_complete_statement', 'batch': None,
                             'scope': 'completeness'})

        missing_keys = [
            {'event_id': item['event_id'], 'batch': item['batch']}
            for item in adjustments
            if item['district'] == name
            and item['kind'] == 'unresolved_missing_key'
            and item['batch'] not in superseded]

        pending_declaration = []
        outside_declared = []
        for fact in own_facts:
            if _in_scope(confirmed_scope, fact['school']):
                continue
            item = {'event_id': fact['event_id'], 'school': fact['school'],
                    'batch': fact['batch']}
            if _in_scope(declared_scope, fact['school']):
                pending_declaration.append(item)
            else:
                outside_declared.append(item)
        if pending_declaration:
            blocking.append({'kind': 'pending_scope', 'batch': None,
                             'scope': 'completeness',
                             'detail': '业务键落在尚未确认完整的声明范围内'})

        in_scope_facts = [fact for fact in own_facts
                          if _in_scope(confirmed_scope, fact['school'])]

        report = {
            'district': name,
            'head': max((batch['sequence'] for batch in own_batches), default=None),
            'declared_scope': declared_scope,
            'confirmed_scope': confirmed_scope,
            'releasable': not blocking,
            'blocking': blocking,
            'deferred': deferred,
            'superseded_batches': sorted(superseded),
            'released_batches': [{'batch': batch['name'],
                                  'sequence': batch['sequence'],
                                  'scope': json.loads(batch['scope'])}
                                 for batch in confirmed],
            'unresolved': {
                'transport': [item for item in blocking if item['scope'] == 'transport'],
                'validation': [item for item in blocking if item['scope'] == 'validation'],
                'completeness': [item for item in blocking if item['scope'] == 'completeness'],
                'missing_keys': missing_keys,
                'pending_declaration': pending_declaration,
                'outside_declared_scope': outside_declared,
            },
            'batches': batch_reports,
            'summary': _summary(in_scope_facts),
            'facts': in_scope_facts,
            'statement': {
                'declared': sum(report['declared_rows'] for report in batch_reports),
                'landed': sum(report['landed_rows'] for report in batch_reports),
                'upserts': sum(report['effects']['upserts'] for report in batch_reports),
                'deletes': sum(report['effects']['deletes'] for report in batch_reports),
                'facts': len(in_scope_facts),
            },
        }
        reports.append(report)
    return {'districts': reports}


def inspect(warehouse):
    facts = warehouse.detail()
    return {
        'summary': _summary(facts),
        'heads': records(warehouse.db, 'SELECT * FROM source_heads ORDER BY district'),
        'statements': records(
            warehouse.db,
            '''SELECT name,district,sequence,mode,declared_rows,checksum,status,
               complete,scope,replaces_batch
               FROM batches ORDER BY district,sequence'''),
        'shards': warehouse.shards(),
        'coverage': records(
            warehouse.db, 'SELECT * FROM coverage ORDER BY district,school'),
        'district_coverage': records(
            warehouse.db, 'SELECT * FROM district_coverage ORDER BY district'),
        'pending': warehouse.pending(),
        'adjustments': warehouse.adjustments(),
        'receipts': warehouse.receipts(),
        'facts': facts,
    }


def _store(warehouse, name, payload):
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    previous = warehouse.db.execute(
        'SELECT payload FROM publications WHERE name=?', (name,)).fetchone()
    if previous and previous[0] != encoded:
        raise ValueError("发布名称不能改写")
    warehouse.db.execute('INSERT OR IGNORE INTO publications VALUES(?,?)',
                         (name, encoded))
    return payload


def publish(warehouse, name):
    """Publish an immutable release of the confirmed facts and summary.

    Districts whose completeness conditions are not met are listed with their
    unresolved ranges and excluded from facts/summary; they are never
    zero-filled or silently dropped.  The payload states the scope each number
    is valid for.
    """
    with atomic(warehouse.db):
        state = reconcile(warehouse)
        payload = {'released': {}, 'deferred': {}}
        for report in state['districts']:
            district = report['district']
            if report['releasable']:
                payload['released'][district] = {
                    'scope': report['confirmed_scope'],
                    'caliber': 'complete_roster',
                    'facts': report['facts'],
                    'summary': report['summary'],
                    'totals': report['statement'],
                    'deferred_ranges': report['deferred'],
                    'outside_declared_scope': report['unresolved']['outside_declared_scope'],
                    'released_batches': report['released_batches'],
                }
            else:
                payload['deferred'][district] = {
                    'declared_scope': report['declared_scope'],
                    'confirmed_scope': report['confirmed_scope'],
                    'reasons': report['blocking'],
                    'unresolved': report['unresolved'],
                }
        return _store(warehouse, name, payload)


def release_district(warehouse, name, district):
    """Publish one district's facts and summary only if it is complete.

    Raises ``IncompleteError`` carrying the unresolved ranges otherwise.  This
    is the strict entry point: it never publishes a partial summary labelled
    as a complete roster.
    """
    with atomic(warehouse.db):
        state = reconcile(warehouse, district=district)
        if not state['districts']:
            raise KeyError(district)
        report = state['districts'][0]
        if not report['releasable']:
            raise IncompleteError(report['unresolved'])
        payload = {
            'scope': report['confirmed_scope'],
            'caliber': 'complete_roster',
            'facts': report['facts'],
            'summary': report['summary'],
            'statement': report['statement'],
            'deferred_ranges': report['deferred'],
            'outside_declared_scope': report['unresolved']['outside_declared_scope'],
            'released_batches': report['released_batches'],
        }
        return _store(warehouse, name, payload)


def read(warehouse, name):
    row = warehouse.db.execute(
        'SELECT payload FROM publications WHERE name=?', (name,)).fetchone()
    if not row:
        raise KeyError(name)
    return json.loads(row[0])
