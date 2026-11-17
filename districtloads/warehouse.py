import json

from .exchange import validate
from .storage import atomic, connect, records

DDL="""
CREATE TABLE IF NOT EXISTS batches(name TEXT PRIMARY KEY,district TEXT,sequence INTEGER,mode TEXT,declared_rows INTEGER,checksum TEXT,raw TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS source_sequence ON batches(district,sequence);
CREATE TABLE IF NOT EXISTS facts(district TEXT,event_id TEXT,school TEXT,day TEXT,people INTEGER,batch TEXT,PRIMARY KEY(district,event_id));
CREATE TABLE IF NOT EXISTS source_heads(district TEXT PRIMARY KEY,sequence INTEGER);
CREATE TABLE IF NOT EXISTS publications(name TEXT PRIMARY KEY,payload TEXT);
"""


class ExchangeWarehouse:
    def __init__(self,root):
        self.db=connect(root,DDL)

    def close(self):
        self.db.close()

    def apply(self,document):
        checksum=validate(document)
        with atomic(self.db):
            old=self.db.execute('SELECT checksum FROM batches WHERE name=?',(document['batch'],)).fetchone()
            if old:
                if old[0]!=checksum:
                    raise ValueError("报送批次内容冲突")
                return False
            previous=self.db.execute('SELECT sequence FROM source_heads WHERE district=?',(document['district'],)).fetchone()
            expected=previous[0]+1 if previous else 1
            if document['sequence']!=expected:
                raise ValueError("源端序号不连续")
            if document['mode']=='full':
                self.db.execute('DELETE FROM facts WHERE district=?',(document['district'],))
            for row in document['rows']:
                if row['op']=='delete':
                    self.db.execute('DELETE FROM facts WHERE district=? AND event_id=?',(document['district'],row['event_id']))
                else:
                    self.db.execute('INSERT OR REPLACE INTO facts VALUES(?,?,?,?,?,?)',(document['district'],row['event_id'],row['school'],row['day'],row['people'],document['batch']))
            self.db.execute('INSERT INTO batches VALUES(?,?,?,?,?,?,?)',(document['batch'],document['district'],document['sequence'],document['mode'],document['declared_rows'],checksum,json.dumps(document,ensure_ascii=False)))
            self.db.execute('INSERT OR REPLACE INTO source_heads VALUES(?,?)',(document['district'],document['sequence']))
        return True

    def detail(self):
        return records(self.db,'SELECT * FROM facts ORDER BY district,event_id')
