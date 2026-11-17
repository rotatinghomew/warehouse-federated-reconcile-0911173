from datetime import date

from .storage import fingerprint, require_text

MODES = ('full', 'delta', 'correction')
ROSTER_MODES = ('full', 'correction')


class SubmissionError(ValueError):
    """A submission cannot be accepted as declared."""


class BatchConflict(SubmissionError):
    """The same batch identity was reused with a different definition/content."""


class ChecksumMismatch(SubmissionError):
    """A shard digest does not match the digest declared in the source manifest."""


class DuplicateKeyConflict(SubmissionError):
    """A business key was already contributed by another shard of the same batch."""


def _scope(document):
    """Return the declared coverage scope, defaulting to the whole district.

    ``district`` means the roster covers every school in the district.
    ``schools`` means the roster only covers the listed schools; schools
    outside that list are not part of this submission at all.
    """
    raw = document.get('scope')
    if raw is None:
        return {'kind': 'district'}
    if not isinstance(raw, dict):
        raise ValueError("覆盖范围声明非法")
    kind = raw.get('kind')
    if kind == 'district':
        return {'kind': 'district'}
    if kind != 'schools':
        raise ValueError("覆盖范围类型未知")
    schools = raw.get('schools')
    if not isinstance(schools, list) or not schools:
        raise ValueError("学校范围需要非空集合")
    normalized = []
    for school in schools:
        normalized.append(require_text(school, '范围学校'))
    if len(normalized) != len(set(normalized)):
        raise ValueError("学校范围内学校重复")
    return {'kind': 'schools', 'schools': normalized}


def _manifest(document):
    raw = document.get('manifest')
    if raw is None:
        return None
    if not isinstance(raw, list) or not raw:
        raise ValueError("传输片清单不能为空")
    manifest = []
    seen = set()
    for entry in raw:
        if not isinstance(entry, dict):
            raise ValueError("传输片清单声明非法")
        shard_id = require_text(entry.get('shard_id'), '传输片标识')
        if shard_id in seen:
            raise ValueError("传输片标识重复")
        seen.add(shard_id)
        checksum = require_text(entry.get('checksum'), '传输片摘要')
        declared_rows = entry.get('declared_rows')
        if not isinstance(declared_rows, int) or isinstance(declared_rows, bool) or declared_rows < 0:
            raise ValueError("传输片声明行数无效")
        manifest.append({'shard_id': shard_id, 'checksum': checksum,
                         'declared_rows': declared_rows})
    return manifest


def _complete(document):
    if 'complete' in document:
        value = document['complete']
        if not isinstance(value, bool):
            raise ValueError("完整性声明必须是布尔值")
        if document.get('mode') == 'delta' and value:
            raise ValueError("增量报送不能声明清册完整")
        return value
    # A full roster (and a correction, which replaces a declared scope) is a
    # closed list by default.  A delta only carries changes and never implies
    # that unmentioned keys were deleted.
    return document.get('mode') in ROSTER_MODES


def scope_covers(scope, school):
    return scope.get('kind') == 'district' or school in set(scope.get('schools', ()))


def canonical_rows(rows):
    """Digest input for rows: only contractual fields, in source order."""
    return [{'event_id': row['event_id'], 'school': row['school'],
             'day': row['day'], 'people': row['people'], 'op': row['op']}
            for row in rows]


def shard_checksum(batch, district, sequence, shard_id, rows):
    """Digest of a shard's actual content, never of its file name.

    A renamed but identical file therefore keeps the same digest, while changed
    content under the same name does not.
    """
    return fingerprint({
        'batch': batch,
        'district': district,
        'sequence': sequence,
        'shard_id': shard_id,
        'rows': canonical_rows(rows),
    })


def content_fingerprint(batch, district, sequence, mode, declared_rows, scope,
                        manifest, replaces_batch, shard_id, rows):
    """Digest of an unsharded submission, deliberately excluding file names.

    ``complete`` is also excluded: a subsequent identical file may upgrade an
    open roster to a closed one without changing the data content.
    """
    return fingerprint({
        'batch': batch,
        'district': district,
        'sequence': sequence,
        'mode': mode,
        'declared_rows': declared_rows,
        'scope': scope,
        'manifest': manifest,
        'replaces_batch': replaces_batch,
        'shard_id': shard_id,
        'rows': canonical_rows(rows),
    })


def _definition(batch, district, sequence, mode, declared_rows, scope, manifest,
                replaces_batch):
    # ``complete`` is deliberately absent: a source may explicitly close an
    # open roster later, but it may not change the batch's identity or scope.
    return {
        'batch': batch,
        'district': district,
        'sequence': sequence,
        'mode': mode,
        'declared_rows': declared_rows,
        'scope': scope,
        'manifest': manifest,
        'replaces_batch': replaces_batch,
    }


def definition_of(submission):
    return _definition(submission['batch'], submission['district'],
                       submission['sequence'], submission['mode'],
                       submission['declared_rows'], submission['scope'],
                       submission['manifest'], submission['replaces_batch'])


def definition_fingerprint_of(submission):
    return fingerprint(definition_of(submission))


def definition_fingerprint(document):
    """Digest of the declared batch identity, scope and manifest."""
    scope = _scope(document)
    return fingerprint(_definition(
        document['batch'], document['district'], document['sequence'],
        document['mode'], document['declared_rows'], scope,
        _manifest(document), document.get('replaces_batch')))


def normalize(document):
    """Validate a submission and return a canonical view used by the store."""
    if not isinstance(document, dict):
        raise ValueError("报送文件必须是对象")
    for field in ('batch', 'district'):
        require_text(document.get(field), field)
    mode = document.get('mode')
    if mode not in MODES:
        raise ValueError("报送模式未知")
    sequence = document.get('sequence')
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise ValueError("源端批次序号无效")
    rows = document.get('rows')
    if not isinstance(rows, list):
        raise ValueError("报送行必须是列表")
    declared_rows = document.get('declared_rows')
    if not isinstance(declared_rows, int) or isinstance(declared_rows, bool) or declared_rows < 0:
        raise ValueError("报送声明行数无效")

    scope = _scope(document)
    if mode == 'correction' and scope['kind'] != 'schools':
        raise ValueError("更正批次必须声明明确的学校范围")

    manifest = _manifest(document)
    shard_id = document.get('shard_id')
    if shard_id is not None:
        require_text(shard_id, '传输片标识')
    if manifest is not None and shard_id is None:
        raise ValueError("带清单的报送必须标明本次传输片")
    if manifest is None:
        if declared_rows != len(rows):
            raise ValueError("报送声明行数不一致")
        if shard_id is not None:
            # A shard-only backfill: its identity, scope and manifest must be
            # resolved against the original batch by the warehouse.
            pass
    else:
        total = sum(entry['declared_rows'] for entry in manifest)
        if declared_rows != total:
            raise ValueError("报送声明行数与传输片清单不一致")
        entry = next((item for item in manifest if item['shard_id'] == shard_id), None)
        if entry is None:
            raise ValueError("本次传输片不在报送清单内")
        if entry['declared_rows'] != len(rows):
            raise ValueError("本次传输片声明行数与实际行数不一致")

    replaces_batch = document.get('replaces_batch')
    if replaces_batch is not None:
        require_text(replaces_batch, '被更正批次')
    file_name = document.get('file_name')
    if file_name is not None:
        require_text(file_name, '文件名')

    normalized_rows = []
    keys = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("报送行必须是对象")
        event_id = require_text(row.get('event_id'), '业务键')
        school = require_text(row.get('school'), '学校')
        op = row.get('op')
        if op not in ('upsert', 'delete'):
            raise ValueError("变更类型未知")
        if mode == 'full' and op == 'delete':
            raise ValueError("完整清册不携带删除事件")
        if not scope_covers(scope, school):
            raise ValueError("报送行学校不在声明覆盖范围内")
        day = row.get('day')
        people = row.get('people')
        if op == 'upsert':
            date.fromisoformat(day)
            if not isinstance(people, int) or isinstance(people, bool) or people < 0:
                raise ValueError("人数非法")
        keys.append(event_id)
        normalized_rows.append({'event_id': event_id, 'school': school,
                                'day': day, 'people': people, 'op': op})
    if len(keys) != len(set(keys)):
        raise ValueError("同一报送业务键重复")

    submission = {
        'batch': document['batch'],
        'district': document['district'],
        'sequence': sequence,
        'mode': mode,
        'declared_rows': declared_rows,
        'rows': normalized_rows,
        'scope': scope,
        'complete': _complete(document),
        'manifest': manifest,
        'shard_id': shard_id,
        'replaces_batch': replaces_batch,
        'file_name': file_name,
        # Digest of actual content (never the file name).  For a shard this is
        # compared with the digest declared in the source manifest; for an
        # unsharded file it is the idempotency key of the batch.
        'content_checksum': (
            shard_checksum(document['batch'], document['district'], sequence,
                           shard_id, normalized_rows)
            if manifest is not None else
            content_fingerprint(document['batch'], document['district'], sequence,
                                mode, declared_rows, scope, manifest,
                                replaces_batch, shard_id, normalized_rows)),
        'document': document,
    }
    submission['definition_checksum'] = definition_fingerprint_of(submission)
    return submission


def validate(document):
    """Backwards-compatible entry point: validate and return a fingerprint."""
    normalize(document)
    return fingerprint(document)
