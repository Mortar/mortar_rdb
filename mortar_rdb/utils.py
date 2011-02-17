from sqlalchemy import Table, Index
from sqlalchemy.schema import RETAIN_SCHEMA, _get_table_key

def tometadata(table, metadata, schema=RETAIN_SCHEMA):
    if schema is RETAIN_SCHEMA:
        schema = table.schema
    key = _get_table_key(table.name, schema)
    if key in metadata.tables:
        raise KeyError('%r is already in %r'%(
                key,metadata
                ))
    args = []
    for c in table.columns:
        args.append(c.copy(schema=schema))
    for c in table.constraints:
        args.append(c.copy(schema=schema))
    table_new = Table(
        table.name, metadata, schema=schema,
        *args, **table.kwargs
        )
    copied_already = set()
    for i in table_new.indexes:
        entry = [i.name,i.unique]
        entry.extend(sorted(i.kwargs.items()))
        entry.extend(i.columns.keys())
        copied_already.add(tuple(entry))
    for i in table.indexes:
        cols = i.columns.keys()
        entry = [i.name,i.unique]
        entry.extend(sorted(i.kwargs.items()))
        entry.extend(cols)
        if tuple(entry) not in copied_already:
            kwargs = dict(i.kwargs)
            kwargs['unique']=i.unique
            Index(i.name,
                  *[getattr(table_new.c,col) for col in cols],
                  **kwargs)
    return table_new
