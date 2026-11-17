from datetime import date

from .storage import fingerprint, require_text


def validate(document):
    for field in ('batch','district'):
        require_text(document[field],field)
    if document['mode'] not in ('full','delta'):
        raise ValueError("报送模式未知")
    if not isinstance(document['sequence'],int) or document['sequence']<1:
        raise ValueError("源端批次序号无效")
    keys=[]
    for row in document['rows']:
        require_text(row['event_id'],'业务键')
        require_text(row['school'],'学校')
        if row['op'] not in ('upsert','delete'):
            raise ValueError("变更类型未知")
        if document['mode']=='full' and row['op']=='delete':
            raise ValueError("完整清册不携带删除事件")
        if row['op']=='upsert':
            date.fromisoformat(row['day'])
            if not isinstance(row['people'],int) or row['people']<0:
                raise ValueError("人数非法")
        keys.append(row['event_id'])
    if len(keys)!=len(set(keys)):
        raise ValueError("同一报送业务键重复")
    if document['declared_rows']!=len(document['rows']):
        raise ValueError("报送声明行数不一致")
    return fingerprint(document)
