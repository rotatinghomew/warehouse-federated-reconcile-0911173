import tempfile
import unittest

from districtloads.exchange import (BatchConflict, ChecksumMismatch,
                                    DuplicateKeyConflict, SubmissionError,
                                    shard_checksum)
from districtloads.reconcile import (IncompleteError, inspect, publish, read,
                                     reconcile, release_district)
from districtloads.warehouse import ExchangeWarehouse


def row(event_id, school='S1', day='2026-11-01', people=4, op='upsert'):
    return {'event_id': event_id, 'school': school, 'day': day,
            'people': people, 'op': op}


def statement(batch, sequence, mode, rows, **options):
    declared = options.pop('declared_rows', None)
    if declared is None:
        parts = options.get('manifest')
        declared = (sum(part['declared_rows'] for part in parts)
                    if parts is not None else len(rows))
    document = {'batch': batch, 'district': '东', 'sequence': sequence,
                'mode': mode, 'declared_rows': declared, 'rows': rows}
    document.update(options)
    return document


def manifest(district, sequence, parts, batch='B1'):
    return [{'shard_id': name,
             'checksum': shard_checksum(batch, district, sequence, name, rows),
             'declared_rows': len(rows)}
            for name, rows in parts]


class WarehouseCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = self.temporary.name
        self.resources = []

    def open(self):
        warehouse = ExchangeWarehouse(self.directory)
        self.resources.append(warehouse)
        return warehouse

    def tearDown(self):
        for resource in reversed(self.resources):
            resource.close()
        self.temporary.cleanup()


class CoverageTest(WarehouseCase):
    def test_missing_shard_blocks_release_and_backfill_joins_original_batch(self):
        warehouse = self.open()
        first, second = [row('E1')], [row('E2', school='S2')]
        parts = manifest('东', 1, [('p1', first), ('p2', second)])

        warehouse.apply(statement('B1', 1, 'full', first, manifest=parts,
                                  shard_id='p1'))
        self.assertEqual(warehouse.detail(), [])
        report = reconcile(warehouse)['districts'][0]
        self.assertFalse(report['releasable'])
        self.assertEqual([item['kind'] for item in report['blocking']],
                         ['missing_shard', 'pending_batch', 'no_complete_statement'])
        self.assertEqual(report['unresolved']['transport'][0]['shard_id'], 'p2')

        with self.assertRaises(IncompleteError) as raised:
            release_district(warehouse, '发布1', '东')
        self.assertEqual(raised.exception.unresolved['transport'][0]['shard_id'], 'p2')

        # A backfill without a manifest is attached to the batch it declares,
        # not treated as a new batch.
        self.assertTrue(warehouse.apply(
            {'batch': 'B1', 'district': '东', 'sequence': 1, 'mode': 'full',
             'declared_rows': 1, 'shard_id': 'p2', 'rows': second,
             'file_name': '迟到补交.json'}))
        self.assertEqual(len(warehouse.batches()), 1)
        self.assertEqual([fact['event_id'] for fact in warehouse.detail()],
                         ['E1', 'E2'])
        self.assertTrue(reconcile(warehouse)['districts'][0]['releasable'])

    def test_same_file_name_with_changed_content_is_not_the_same_success(self):
        warehouse = self.open()
        first = [row('E1')]
        parts = manifest('东', 1, [('p1', first)])
        warehouse.apply(statement('B1', 1, 'full', first, manifest=parts,
                                  shard_id='p1', file_name='报送.json'))
        self.assertFalse(warehouse.apply(
            statement('B1', 1, 'full', first, manifest=parts,
                      shard_id='p1', file_name='副本.json')))

        changed = [row('E1', people=9)]
        with self.assertRaises(ChecksumMismatch):
            warehouse.apply(statement('B1', 1, 'full', changed, manifest=parts,
                                      shard_id='p1', file_name='报送.json'))
        self.assertEqual([fact['people'] for fact in warehouse.detail()], [4])
        mismatch = reconcile(warehouse)['districts'][0]['unresolved']['validation']
        self.assertEqual(mismatch[0]['kind'], 'checksum_mismatch')

    def test_checksum_and_row_count_disagree_differently(self):
        warehouse = self.open()
        first = [row('E1')]
        bad_digest = [{'shard_id': 'p1', 'checksum': '0' * 64, 'declared_rows': 1}]
        with self.assertRaises(ChecksumMismatch):
            warehouse.apply(statement('B1', 1, 'full', first, manifest=bad_digest,
                                      shard_id='p1'))
        report = reconcile(warehouse)['districts'][0]
        self.assertEqual(report['unresolved']['validation'][0]['kind'],
                         'checksum_mismatch')
        self.assertEqual(report['batches'][0]['differences'][0]['declared_checksum'],
                         '0' * 64)

    def test_duplicate_business_key_across_shards_is_a_validation_failure(self):
        warehouse = self.open()
        first, second = [row('E1')], [row('E1', school='S2')]
        parts = manifest('东', 1, [('p1', first), ('p2', second)])
        warehouse.apply(statement('B1', 1, 'full', first, manifest=parts,
                                  shard_id='p1'))
        with self.assertRaises(DuplicateKeyConflict):
            warehouse.apply(statement('B1', 1, 'full', second, manifest=parts,
                                      shard_id='p2'))
        report = reconcile(warehouse)['districts'][0]
        self.assertEqual(report['unresolved']['validation'][0]['kind'], 'key_conflict')
        self.assertEqual(warehouse.detail(), [])

    def test_backfill_does_not_close_an_open_roster_by_default(self):
        warehouse = self.open()
        first, second = [row('E1')], [row('E2', school='S2')]
        parts = manifest('东', 1, [('p1', first), ('p2', second)])
        warehouse.apply(statement('B1', 1, 'full', first, manifest=parts,
                                  shard_id='p1', complete=False))
        warehouse.apply({'batch': 'B1', 'district': '东', 'sequence': 1,
                         'mode': 'full', 'declared_rows': 1,
                         'shard_id': 'p2', 'rows': second})
        self.assertFalse(warehouse.batches()[0]['complete'])
        self.assertFalse(reconcile(warehouse)['districts'][0]['releasable'])

    def test_backfill_cannot_rewrite_batch_identity_or_scope(self):
        warehouse = self.open()
        first, second = [row('E1')], [row('E2', school='S2')]
        parts = manifest('东', 1, [('p1', first), ('p2', second)])
        warehouse.apply(statement('B1', 1, 'full', first, manifest=parts,
                                  shard_id='p1'))
        with self.assertRaises(BatchConflict):
            warehouse.apply({'batch': 'B1', 'district': '东', 'sequence': 2,
                             'mode': 'full', 'declared_rows': 1,
                             'shard_id': 'p2', 'rows': second})
        with self.assertRaises(SubmissionError):
            warehouse.apply({'batch': 'B1', 'district': '东', 'sequence': 1,
                             'mode': 'full', 'declared_rows': 1,
                             'shard_id': 'unknown', 'rows': second})
        with self.assertRaises(BatchConflict):
            warehouse.apply({'batch': 'B1', 'district': '东', 'sequence': 1,
                             'mode': 'full', 'declared_rows': 1,
                             'shard_id': 'p2', 'rows': second,
                             'scope': {'kind': 'schools', 'schools': ['S2']}})


class ScopeTest(WarehouseCase):
    def test_out_of_scope_school_survives_and_is_reported(self):
        warehouse = self.open()
        warehouse.apply(statement('B1', 1, 'full', [row('E1'), row('E2', school='S2')]))
        warehouse.apply(statement('B2', 2, 'full', [row('E3', school='S1')],
                                  scope={'kind': 'schools', 'schools': ['S1']}))
        self.assertEqual([fact['event_id'] for fact in warehouse.detail()],
                         ['E2', 'E3'])

        report = reconcile(warehouse)['districts'][0]
        self.assertEqual(report['confirmed_scope'], {'kind': 'district'})
        self.assertEqual({item['event_id'] for item in
                          report['unresolved']['outside_declared_scope']}, set())
        self.assertEqual(report['summary'],
                         [{'district': '东', 'school': 'S1', 'events': 1, 'people': 4},
                          {'district': '东', 'school': 'S2', 'events': 1, 'people': 4}])

    def test_only_declared_scope_is_ever_treated_as_a_complete_caliber(self):
        warehouse = self.open()
        warehouse.apply(statement('B1', 1, 'full', [row('E1', school='S1')],
                                  scope={'kind': 'schools', 'schools': ['S1']}))
        warehouse.apply(statement('B2', 2, 'delta', [row('E2', school='S2')]))
        report = reconcile(warehouse)['districts'][0]
        self.assertEqual(report['confirmed_scope'],
                         {'kind': 'schools', 'schools': ['S1']})
        self.assertTrue(report['releasable'])
        payload = publish(warehouse, '摘录')
        self.assertEqual({fact['event_id'] for fact in
                          payload['released']['东']['facts']}, {'E1'})
        self.assertEqual({item['event_id'] for item in
                          payload['released']['东']['outside_declared_scope']}, {'E2'})

    def test_open_roster_omission_is_not_a_delete(self):
        warehouse = self.open()
        warehouse.apply(statement('B1', 1, 'full', [row('E1'), row('E2')]))
        warehouse.apply(statement('B2', 2, 'full', [row('E1')], complete=False))
        self.assertEqual([fact['event_id'] for fact in warehouse.detail()],
                         ['E1', 'E2'])
        report = reconcile(warehouse)['districts'][0]
        self.assertFalse(report['releasable'])
        self.assertIn({'event_id': 'E2', 'batch': 'B2'},
                      report['unresolved']['missing_keys'])
        self.assertIn('open_roster', [item['kind'] for item in report['blocking']])

        # Closing the same roster is allowed and then performs the omission.
        warehouse.apply(statement('B3', 3, 'full', [row('E1')], complete=True))
        self.assertEqual([fact['event_id'] for fact in warehouse.detail()], ['E1'])

    def test_closed_district_roster_removes_absent_keys_in_scope(self):
        warehouse = self.open()
        warehouse.apply(statement('B1', 1, 'full', [row('E1'), row('E2', school='S2')]))
        warehouse.apply(statement('B2', 2, 'full', [row('E1')]))
        self.assertEqual([fact['event_id'] for fact in warehouse.detail()], ['E1'])
        tombstones = warehouse.tombstones()
        self.assertEqual([(item['event_id'], item['deleted_reason'])
                          for item in tombstones], [('E2', 'roster_omission')])


class OrderingTest(WarehouseCase):
    def test_late_backfill_cannot_overwrite_later_confirmed_data(self):
        warehouse = self.open()
        first, second = [row('E1')], [row('E2', school='S2')]
        parts = manifest('东', 1, [('p1', first), ('p2', second)])
        warehouse.apply(statement('B1', 1, 'full', first, manifest=parts,
                                  shard_id='p1'))
        warehouse.apply(statement('B2', 2, 'delta', [row('E2', school='S2',
                                                         day='2026-11-05',
                                                         people=8)]))
        warehouse.apply({'batch': 'B1', 'district': '东', 'sequence': 1,
                         'mode': 'full', 'declared_rows': 1,
                         'shard_id': 'p2', 'rows': second})
        self.assertEqual([(fact['event_id'], fact['people'])
                          for fact in warehouse.detail()],
                         [('E1', 4), ('E2', 8)])
        self.assertIn(('E2', 'skipped_stale_upsert'),
                      [(item['event_id'], item['kind'])
                       for item in warehouse.adjustments()])

    def test_late_backfill_cannot_resurrect_a_later_deletion(self):
        warehouse = self.open()
        first, second = [row('E1')], [row('E2', school='S2')]
        parts = manifest('东', 1, [('p1', first), ('p2', second)])
        warehouse.apply(statement('B1', 1, 'full', first, manifest=parts,
                                  shard_id='p1'))
        warehouse.apply(statement('B2', 2, 'full', [row('E1')]))
        warehouse.apply({'batch': 'B1', 'district': '东', 'sequence': 1,
                         'mode': 'full', 'declared_rows': 1,
                         'shard_id': 'p2', 'rows': second})
        self.assertEqual([fact['event_id'] for fact in warehouse.detail()], ['E1'])
        self.assertIn(('E2', 'skipped_stale_upsert'),
                      [(item['event_id'], item['kind'])
                       for item in warehouse.adjustments()])

    def test_correction_upsert_wins_inside_its_scope_but_not_stale_gaps(self):
        warehouse = self.open()
        warehouse.apply(statement('B1', 1, 'full', [row('E1')]))
        warehouse.apply(statement('B2', 2, 'delta', [row('E1', day='2026-11-02',
                                                         people=7)]))
        # A correction is a new, explicit statement: inside its declared scope
        # it replaces the corrected batch's content.
        warehouse.apply(statement('B3', 3, 'correction', [row('E1', people=9)],
                                  scope={'kind': 'schools', 'schools': ['S1']},
                                  replaces_batch='B1'))
        self.assertEqual([(fact['event_id'], fact['people'])
                          for fact in warehouse.detail()], [('E1', 9)])

        # An older backfill still cannot roll a later confirmation back.
        first = [row('E0')]
        second = [row('E1', people=4)]
        parts = manifest('东', 1, [('p1', first), ('p2', second)])
        warehouse2 = ExchangeWarehouse(tempfile.mkdtemp())
        self.resources.append(warehouse2)
        warehouse2.apply(statement('B1', 1, 'full', first, manifest=parts,
                                   shard_id='p1'))
        warehouse2.apply(statement('B2', 2, 'delta',
                                   [row('E1', day='2026-11-02', people=7)]))
        warehouse2.apply({'batch': 'B1', 'district': '东', 'sequence': 1,
                          'mode': 'full', 'declared_rows': 1,
                          'shard_id': 'p2', 'rows': second})
        self.assertEqual([(fact['event_id'], fact['people'])
                          for fact in warehouse2.detail()],
                         [('E0', 4), ('E1', 7)])
        self.assertIn(('E1', 'skipped_stale_upsert'),
                      [(item['event_id'], item['kind'])
                       for item in warehouse2.adjustments()])


class CorrectionTest(WarehouseCase):
    def test_correction_replaces_only_the_declared_batch_scope(self):
        warehouse = self.open()
        warehouse.apply(statement('B1', 1, 'full', [row('E1', school='S1'),
                                                    row('E2', school='S2')]))
        warehouse.apply(statement('B2', 2, 'delta', [row('E3', school='S3')]))
        warehouse.apply(statement('B3', 3, 'correction', [row('E1', school='S1',
                                                              people=9)],
                                  scope={'kind': 'schools', 'schools': ['S1']},
                                  replaces_batch='B1'))
        self.assertEqual([(fact['event_id'], fact['people'], fact['batch'])
                          for fact in warehouse.detail()],
                         [('E1', 9, 'B3'), ('E2', 4, 'B1'), ('E3', 4, 'B2')])

    def test_correction_requires_explicit_scope_and_target(self):
        warehouse = self.open()
        warehouse.apply(statement('B1', 1, 'full', [row('E1')]))
        with self.assertRaises(ValueError):
            warehouse.apply(statement('B2', 2, 'correction', [row('E1')]))
        with self.assertRaises(SubmissionError):
            warehouse.apply(statement('B2', 2, 'correction', [row('E1')],
                                      scope={'kind': 'schools',
                                             'schools': ['S1']}))
        with self.assertRaises(SubmissionError):
            warehouse.apply(statement('B2', 2, 'correction', [row('E1')],
                                      scope={'kind': 'schools',
                                             'schools': ['S1']},
                                      replaces_batch='missing'))
        # A correction may not widen beyond the range it replaces.
        warehouse.apply(statement('B2', 2, 'full', [row('E1', school='S1')],
                                  scope={'kind': 'schools', 'schools': ['S1']}))
        with self.assertRaises(SubmissionError):
            warehouse.apply(statement('B3', 3, 'correction',
                                      [row('E9', school='S9')],
                                      scope={'kind': 'schools',
                                             'schools': ['S9']},
                                      replaces_batch='B2'))

    def test_explicit_delete_reaches_downstream(self):
        warehouse = self.open()
        warehouse.apply(statement('B1', 1, 'full', [row('E1'), row('E2')]))
        warehouse.apply(statement('B2', 2, 'delta', [row('E2', op='delete')]))
        self.assertEqual([fact['event_id'] for fact in warehouse.detail()], ['E1'])
        self.assertEqual([(item['event_id'], item['deleted_reason'])
                          for item in warehouse.tombstones()],
                         [('E2', 'explicit')])
        payload = publish(warehouse, '删除后')
        self.assertEqual([fact['event_id']
                          for fact in payload['released']['东']['facts']], ['E1'])


class ReleaseTest(WarehouseCase):
    def test_deferred_district_is_not_zero_filled_or_partially_released(self):
        warehouse = self.open()
        first, second = [row('E1')], [row('E2', school='S2')]
        parts = manifest('东', 1, [('p1', first), ('p2', second)])
        warehouse.apply(statement('B1', 1, 'full', first, manifest=parts,
                                  shard_id='p1'))
        payload = publish(warehouse, '首次')
        self.assertEqual(payload['released'], {})
        self.assertEqual(list(payload['deferred']), ['东'])
        deferred = payload['deferred']['东']
        self.assertEqual(deferred['unresolved']['transport'][0]['shard_id'], 'p2')
        self.assertEqual(read(warehouse, '首次'), payload)

        warehouse.apply({'batch': 'B1', 'district': '东', 'sequence': 1,
                         'mode': 'full', 'declared_rows': 1,
                         'shard_id': 'p2', 'rows': second})
        with self.assertRaises(ValueError):
            publish(warehouse, '首次')
        complete = publish(warehouse, '补全后')
        self.assertEqual({fact['event_id']
                          for fact in complete['released']['东']['facts']},
                         {'E1', 'E2'})
        self.assertEqual(complete['deferred'], {})

    def test_statement_landing_warehouse_and_summary_are_traceable(self):
        warehouse = self.open()
        warehouse.apply(statement('B1', 1, 'full', [row('E1'), row('E2')]))
        warehouse.apply(statement('B2', 2, 'delta', [row('E3'),
                                                     row('E2', op='delete')]))
        report = reconcile(warehouse)['districts'][0]
        self.assertEqual(report['statement']['declared'], 4)
        self.assertEqual(report['statement']['landed'], 4)
        self.assertEqual(report['statement']['upserts'], 3)
        self.assertEqual(report['statement']['deletes'], 1)
        self.assertEqual(report['statement']['facts'], 2)
        self.assertEqual([(item['school'], item['events'], item['people'])
                          for item in report['summary']], [('S1', 2, 8)])

        state = inspect(warehouse)
        self.assertEqual({item['shard_id'] for item in state['shards']},
                         {'__whole__'})
        self.assertEqual(len(state['batches'] if 'batches' in state else
                             state['statements']), 2)

    def test_superseded_gap_does_not_block_forever_but_reports_itself(self):
        warehouse = self.open()
        first, second = [row('E1')], [row('E2', school='S2')]
        parts = manifest('东', 1, [('p1', first), ('p2', second)])
        warehouse.apply(statement('B1', 1, 'full', first, manifest=parts,
                                  shard_id='p1'))
        warehouse.apply(statement('B2', 2, 'full', [row('E1')]))
        report = reconcile(warehouse)['districts'][0]
        self.assertEqual(report['superseded_batches'], ['B1'])
        self.assertTrue(report['releasable'])
        self.assertIn('superseded_gap',
                      [item['kind'] for item in report['deferred']])
        self.assertEqual(report['unresolved']['transport'], [])

    def test_district_head_is_a_watermark_not_the_last_receipt(self):
        warehouse = self.open()
        first, second = [row('E1')], [row('E2')]
        parts = manifest('东', 1, [('p1', first), ('p2', second)])
        warehouse.apply(statement('B1', 1, 'full', first, manifest=parts,
                                  shard_id='p1'))
        warehouse.apply(statement('B2', 2, 'delta', [row('E3')]))
        warehouse.apply({'batch': 'B1', 'district': '东', 'sequence': 1,
                         'mode': 'full', 'declared_rows': 1,
                         'shard_id': 'p2', 'rows': second})
        heads = inspect(warehouse)['heads']
        self.assertEqual([(item['district'], item['sequence']) for item in heads],
                         [('东', 2)])
        self.assertTrue(reconcile(warehouse)['districts'][0]['releasable'])


class LegacyTest(unittest.TestCase):
    def test_existing_database_is_migrated(self):
        import sqlite3
        with tempfile.TemporaryDirectory() as directory:
            database = sqlite3.connect(f'{directory}/catalog.sqlite3')
            database.executescript('''
                CREATE TABLE batches(name TEXT PRIMARY KEY,district TEXT,sequence INTEGER,
                    mode TEXT,declared_rows INTEGER,checksum TEXT,raw TEXT);
                CREATE UNIQUE INDEX source_sequence ON batches(district,sequence);
                CREATE TABLE facts(district TEXT,event_id TEXT,school TEXT,day TEXT,
                    people INTEGER,batch TEXT,PRIMARY KEY(district,event_id));
                CREATE TABLE source_heads(district TEXT PRIMARY KEY,sequence INTEGER);
                CREATE TABLE publications(name TEXT PRIMARY KEY,payload TEXT);
                INSERT INTO batches VALUES('旧1','东',1,'full',1,'x','{}');
                INSERT INTO facts VALUES('东','E1','S1','2026-11-01',4,'旧1');
                INSERT INTO source_heads VALUES('东',1);
            ''')
            database.commit()
            database.close()

            warehouse = ExchangeWarehouse(directory)
            try:
                self.assertEqual([(item['event_id'], item['sequence'])
                                  for item in warehouse.detail()], [('E1', 1)])
                warehouse.apply(statement('B2', 2, 'delta', [row('E2')]))
                self.assertEqual([fact['event_id'] for fact in warehouse.detail()],
                                 ['E1', 'E2'])
            finally:
                warehouse.close()
