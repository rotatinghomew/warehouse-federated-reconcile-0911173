from .warehouse import ExchangeWarehouse
from .reconcile import publish


def run(root):
    warehouse=ExchangeWarehouse(root)
    try:
        warehouse.apply({'batch':'报送1','district':'东','sequence':1,'mode':'full','declared_rows':1,'rows':[
            {'event_id':'E1','school':'S1','day':'2026-11-01','people':4,'op':'upsert'}]})
        return publish(warehouse,'地区摘录')
    finally:
        warehouse.close()
