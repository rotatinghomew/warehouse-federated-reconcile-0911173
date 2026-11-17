from .exchange import shard_checksum
from .reconcile import publish
from .warehouse import ExchangeWarehouse


def run(root):
    warehouse = ExchangeWarehouse(root)
    try:
        first = [{'event_id': 'E1', 'school': 'S1', 'day': '2026-11-01',
                  'people': 4, 'op': 'upsert'}]
        second = [{'event_id': 'E2', 'school': 'S2', 'day': '2026-11-01',
                   'people': 2, 'op': 'upsert'}]
        manifest = [
            {'shard_id': 'p1', 'declared_rows': len(first),
             'checksum': shard_checksum('报送1', '东', 1, 'p1', first)},
            {'shard_id': 'p2', 'declared_rows': len(second),
             'checksum': shard_checksum('报送1', '东', 1, 'p2', second)},
        ]
        warehouse.apply({'batch': '报送1', 'district': '东', 'sequence': 1,
                         'mode': 'full', 'declared_rows': 2,
                         'manifest': manifest, 'shard_id': 'p1',
                         'file_name': '东-变1-片1.json', 'rows': first})
        # The second shard is still in transit; the first release therefore
        # defers this district instead of publishing a partial total.
        partial = publish(warehouse, '首次缺口')
        warehouse.apply({'batch': '报送1', 'district': '东', 'sequence': 1,
                         'mode': 'full', 'declared_rows': 1,
                         'shard_id': 'p2', 'file_name': '东-变1-片1补.json',
                         'rows': second})
        complete = publish(warehouse, '补全后')
        return {'deferred': partial['deferred'], 'released': complete['released']}
    finally:
        warehouse.close()
