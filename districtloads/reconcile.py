import json

from .storage import atomic, records


def inspect(warehouse):
    summary=records(warehouse.db,'SELECT district,school,COUNT(*) events,SUM(people) people FROM facts GROUP BY district,school ORDER BY district,school')
    heads=records(warehouse.db,'SELECT * FROM source_heads ORDER BY district')
    statements=records(warehouse.db,'SELECT name,district,sequence,mode,declared_rows,checksum FROM batches ORDER BY district,sequence')
    return {'summary':summary,'heads':heads,'statements':statements,'facts':warehouse.detail()}


def publish(warehouse,name):
    with atomic(warehouse.db):
        payload=inspect(warehouse)
        encoded=json.dumps(payload,ensure_ascii=False,sort_keys=True)
        previous=warehouse.db.execute('SELECT payload FROM publications WHERE name=?',(name,)).fetchone()
        if previous and previous[0]!=encoded:
            raise ValueError("发布名称不能改写")
        warehouse.db.execute('INSERT OR IGNORE INTO publications VALUES(?,?)',(name,encoded))
    return payload


def read(warehouse,name):
    row=warehouse.db.execute('SELECT payload FROM publications WHERE name=?',(name,)).fetchone()
    if not row:
        raise KeyError(name)
    return json.loads(row[0])
