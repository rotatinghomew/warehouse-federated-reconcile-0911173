import tempfile
import unittest


class WorkspaceTest(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.directory=self.temporary.name
        self.resources=[]

    def open(self, factory):
        resource=factory(self.directory)
        self.resources.append(resource)
        return resource

    def tearDown(self):
        for resource in reversed(self.resources):
            resource.close()
        self.temporary.cleanup()

from districtloads.warehouse import ExchangeWarehouse
from districtloads.reconcile import inspect, publish, read


def report(batch='B1',sequence=1,mode='full',rows=None):
    if rows is None:
        rows=[{'event_id':'E1','school':'S1','day':'2026-11-01','people':4,'op':'upsert'}]
    return {'batch':batch,'district':'东','sequence':sequence,'mode':mode,'declared_rows':len(rows),'rows':rows}


class Behavior(WorkspaceTest):
    def test_source_sequence_and_checksum(self):
        warehouse=self.open(ExchangeWarehouse)
        self.assertTrue(warehouse.apply(report()))
        self.assertFalse(warehouse.apply(report()))
        with self.assertRaises(ValueError):
            warehouse.apply(dict(report(),declared_rows=2))
        with self.assertRaises(ValueError):
            warehouse.apply(report('B3',3))
        self.assertEqual(len(warehouse.detail()),1)

    def test_full_and_delta_deletion(self):
        warehouse=self.open(ExchangeWarehouse)
        warehouse.apply(report())
        warehouse.apply(report('B2',2,'delta',[{'event_id':'E1','school':'S1','op':'delete'}]))
        self.assertEqual(warehouse.detail(),[])
        warehouse.apply(report('B3',3,'full'))
        warehouse.apply(report('B4',4,'full',[]))
        self.assertEqual(warehouse.detail(),[])

    def test_other_district_isolation(self):
        warehouse=self.open(ExchangeWarehouse)
        warehouse.apply(report())
        warehouse.apply(dict(report('W1'),district='西'))
        warehouse.apply(report('B2',2,'full',[]))
        self.assertEqual([row['district'] for row in warehouse.detail()],['西'])

    def test_reconciliation_and_release(self):
        warehouse=self.open(ExchangeWarehouse)
        warehouse.apply(report())
        old=publish(warehouse,'首次')
        self.assertEqual(inspect(warehouse)['summary'][0]['people'],4)
        warehouse.apply(report('B2',2,'delta',[{'event_id':'E2','school':'S2','day':'2026-11-01','people':2,'op':'upsert'}]))
        reopened=self.open(ExchangeWarehouse)
        self.assertEqual(read(reopened,'首次'),old)
        with self.assertRaises(ValueError):
            publish(reopened,'首次')
