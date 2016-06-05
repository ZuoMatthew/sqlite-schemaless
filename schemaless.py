"""
SQLite-Schemaless

Schemaless database built on top of SQLite. I based the design of
`sqlite-schemaless` on the data-store described an Uber engineering post
(https://eng.uber.com/schemaless-part-one/). All data is stored in a single
SQLite database, which can either be on-disk or in-memory.

Data is organized by *KeySpace*. A KeySpace might be something like "users"
or "tweets". Inside each KeySpace there are *Rows*. A row is identified by an
integer `row_key` and consists of one or more named columns. In these columns
you can store arbitrary JSON blobs. So:

* KeySpace1:
    * Row1:
        * ColumnA: {arbitrary json data}
        * ColumnB: {more json data}
    * Row2:
        * ColumnA: {json data}
        * ColumnC: {json data}
    * Row3:
        * ColumnA: {json data}
        * ColumnB: {json data}

You can create indexes on values stored in the JSON column data, then the
secondary indexes can be used to construct queries based on values nested in
the JSON blobs.

`sqlite-schemaless` also allows you to bind event handlers that will execute
whenever data is inserted or updated in a keyspace.
"""
import operator
import re
import sys
import time
from collections import defaultdict
from collections import namedtuple

from peewee import *
from peewee import sqlite3 as _sqlite3
from playhouse.sqlite_ext import *


if sys.version_info[0] == 3:
    basestring = str


USE_JSON_FALLBACK = True
if _sqlite3.sqlite_version_info >= (3, 9, 0):
    conn = _sqlite3.connect(':memory:')
    try:
        conn.execute('select json(?)', (1337,))
    except:
        pass
    else:
        USE_JSON_FALLBACK = False
    conn.close()

split_re = re.compile('(?:(\[\d+\])|\.)')


def _json_extract_fallback(json_text, path):
    json_data = json.loads(json_text)
    path = path.lstrip('$.')
    parts = split_re.split(path)
    for part in filter(None, parts):
        try:
            if part.startswith('['):
                json_data = json_data[int(part.strip('[]'))]
            else:
                json_data = json_data[part]
        except (KeyError, IndexError):
            return None
    return json_data


class Schemaless(SqliteExtDatabase):
    def __init__(self, filename, wal_mode=True, cache_size=4000,
                 use_json_fallback=USE_JSON_FALLBACK, **kwargs):
        pragmas = [('cache_size', cache_size)]
        if wal_mode:
            pragmas.append(('journal_mode', 'wal'))
        super(Schemaless, self).__init__(filename, pragmas=pragmas, **kwargs)
        self._handlers = defaultdict(list)
        self.func('emit_event')(self.event_handler)
        self._json_fallback = use_json_fallback
        if self._json_fallback:
            self.func('json_extract', _json_extract_fallback)

    def event_handler(self, table, row_key, column, value):
        for handler in self._handlers[table]:
            if handler(table, row_key, column, json.loads(value)) is False:
                break

    def bind_handler(self, keyspace, handler):
        self._handlers[keyspace.db_table].append(handler)

    def handler(self, *keyspaces):
        def decorator(fn):
            for keyspace in keyspaces:
                self.bind_handler(keyspace.db_table, fn)
            return fn
        return decorator

    def keyspace(self, item, *indexes):
        return KeySpace(self, item, *indexes)


def clean(s):
    return re.sub('[^\w]+', '', s)


def row_iterator(keyspace, query):
    curr = None
    accum = {}
    for row_key, column, value in query:
        if curr is None:
            curr = row_key
        if row_key != curr:
            row = Row(
                keyspace=keyspace,
                identifier=curr,
                **accum)
            curr = row_key
            yield row
            accum = {}
        accum[column] = value
    if accum:
        yield Row(keyspace=keyspace, identifier=row_key, **accum)


class IndexQuery(object):
    def __init__(self, index, expression, operations=None, reverse=False):
        self.index = index
        self.expression = expression
        self.reverse = reverse
        self.query_operations = operations or []

    def clone(self):
        return IndexQuery(
            self.index,
            self.expression,
            list(self.query_operations),
            self.reverse)

    def __neg__(self):
        clone = self.clone()
        clone.reverse = not self.reverse
        return clone

    def __or__(self, rhs):
        clone = self.clone()
        if rhs.index.name == self.index.name:
            clone.expression = (clone.expression | rhs.expression)
        else:
            clone.query_operations.append((operator.or_, rhs))
        return clone

    def __and__(self, rhs):
        clone = self.clone()
        if rhs.index == self.index:
            clone.expression = (clone.expression & rhs.expression)
        else:
            clone.query_operations.append((operator.and_, rhs))
        return clone

    def query(self):
        model = self.index.keyspace.model
        query = (model
                 .select(
                     model.row_key,
                     model.column,
                     model.value)
                 .join(
                     self.index.model,
                     on=(self.index.model.row_key == model.row_key))
                 .where(self.expression)
                 .group_by(
                     model.row_key,
                     model.column))

        for op, idx_query in self.query_operations:
            query = op(query, idx_query.query())

        return query

    def __iter__(self):
        query = self.query()
        if self.reverse:
            query = query.order_by(SQL('1 DESC'))
        else:
            query = query.order_by(SQL('1'))
        for row in row_iterator(self.index.keyspace, query.tuples()):
            yield row


class _QueryDescriptor(object):
    def __get__(self, instance, instance_type=None):
        if instance:
            return instance.model.value
        return self


class Index(object):
    _op_map = {
        '<': operator.lt,
        '<=': operator.le,
        '==': operator.eq,
        '>=': operator.ge,
        '>': operator.gt,
        '!=': operator.ne,
        'LIKE': operator.pow,
        'IN': operator.lshift,
    }

    def __init__(self, column, path):
        self.column = column
        self.path = path
        self.name = clean(path)
        self.keyspace = None

    def bind(self, keyspace):
        self.keyspace = keyspace
        self.db_table = '%s_%s_%s' % (
            self.keyspace.db_table,
            clean(self.column),
            self.name)
        self.model = self.get_model_class()

    def get_model_class(self):
        class BaseModel(Model):
            row_key = IntegerField(unique=True)
            value = TextField(null=True)

            class Meta:
                database = self.keyspace.database

        class Meta:
            db_table = self.db_table

        return type(self.name, (BaseModel,), {'Meta': Meta})

    def all_items(self):
        return (self.model
                .select(self.model.row_key, self.model.value)
                .order_by(self.model.row_key)
                .dicts())

    def _create_triggers(self):
        self.model.create_table(True)
        trigger_name = '%s_populate' % self.name
        query = (
            'CREATE TRIGGER IF NOT EXISTS %(trigger_name)s '
            'AFTER INSERT ON %(keyspace)s '
            'FOR EACH ROW WHEN ('
            'new.column = \'%(column)s\' AND '
            'json_extract(new.value, \'%(path)s\') IS NOT NULL) '
            'BEGIN '
            'INSERT OR REPLACE INTO %(index)s (row_key, value) '
            'VALUES (new.row_key, json_extract(new.value, \'%(path)s\')); '
            'END') % {
                'trigger_name': trigger_name,
                'keyspace': self.keyspace.db_table,
                'column': self.column,
                'index': self.db_table,
                'path': self.path}
        self.keyspace.database.execute_sql(query)

        trigger_name = '%s_delete' % self.name
        query = (
            'CREATE TRIGGER IF NOT EXISTS %(trigger_name)s '
            'BEFORE DELETE ON %(keyspace)s '
            'FOR EACH ROW WHEN OLD.column = \'%(column)s\' BEGIN '
            'DELETE FROM %(index)s WHERE '
            'row_key = OLD.row_key; '
            'END') % {
                'trigger_name': trigger_name,
                'keyspace': self.keyspace.db_table,
                'column': self.column,
                'index': self.db_table}
        self.keyspace.database.execute_sql(query)

    def _drop_triggers(self):
        for name in ('_populate', '_delete'):
            self.keyspace.database.execute_sql('DROP TRIGGER IF EXISTS %s%s' %
                                               (self.name, name))

    def _populate(self):
        query = (
            'INSERT INTO %(index)s (row_key, value) '
            'SELECT k.row_key, json_extract(k.value, \'%(path)s\') '
            'FROM %(keyspace)s AS k '
            'WHERE ('
            'k.column = ? AND '
            'json_extract(k.value, \'%(path)s\') IS NOT NULL)') % {
                'keyspace': self.keyspace.db_table,
                'index': self.db_table,
                'column': self.column,
                'path': self.path}
        self.keyspace.database.execute_sql(query, (self.column,))

    def query(self, value, operation=operator.eq, reverse=False):
        # Support string operations in addition to functional, for readability.
        if isinstance(value, Expression):
            return IndexQuery(self, value, reverse=reverse)
        if isinstance(operation, basestring):
            operation = self._op_map[operation]
        expression = operation(self.model.value, value)
        return IndexQuery(self, expression, reverse=reverse)

    def _e(op):
        def inner(self, rhs):
            return self.query(rhs, op)
        return inner
    __eq__ = _e('==')
    __ne__ = _e('!=')
    __gt__ = _e('>')
    __ge__ = _e('>=')
    __lt__ = _e('<')
    __le__ = _e('<=')

    v = _QueryDescriptor()


class KeySpace(object):
    def __init__(self, database, name, *indexes):
        self.database = database
        self.name = name
        self.db_table = clean(self.name)
        self.model = self.get_model_class()
        self.indexes = []
        for index in indexes:
            index.bind(self)
            self.indexes.append(index)

    def add_index(self, index):
        # Add index to existing KeySpace.
        index.bind(self)
        index._create_triggers()
        index._populate()
        self.indexes.append(index)

    def handler(self, fn):
        def wrapper(table, row_key, column, value):
            return fn(row_key, column, value)
        self.database.bind_handler(self, wrapper)
        def unbind():
            self.database._handlers[self.db_table].remove(wrapper)
        fn.unbind = unbind
        return fn

    def get_model_class(self):
        class BaseModel(Model):
            row_key = IntegerField(index=True)
            column = TextField(index=True)
            value = JSONField(null=True)
            timestamp = FloatField(default=time.time, index=True)

            class Meta:
                database = self.database
                indexes = (
                    (('row_key', 'column'), True),
                )

        class Meta:
            db_table = self.db_table

        return type(self.name, (BaseModel,), {'Meta': Meta})

    def create(self):
        self.model.create_table(True)
        self._create_trigger()
        for index in self.indexes:
            index._create_triggers()

    def drop(self):
        for index in self.indexes:
            index._drop_triggers()
        self._drop_trigger()
        self.model.drop_table()

    def _create_trigger(self):
        trigger_name = '%s_signal' % self.db_table
        query = (
            'CREATE TRIGGER IF NOT EXISTS %(trigger_name)s '
            'AFTER INSERT ON %(keyspace)s '
            'FOR EACH ROW BEGIN '
            'SELECT emit_event('
            '\'%(keyspace)s\', new.row_key, new.column, new.value);'
            'END') % {
                'trigger_name': trigger_name,
                'keyspace': self.db_table,
            }
        self.database.execute_sql(query)

    def _drop_trigger(self):
        trigger_name = '%s_signal' % self.db_table
        self.database.execute_sql('DROP TRIGGER IF EXISTS %s' % trigger_name)

    def __getitem__(self, identifier):
        return Row(self, identifier)

    def __delitem__(self, identifier):
        row = Row(self, identifier)
        return row.delete()

    def get_row(self, identifier, preload=None):
        return Row(self, identifier, preload=preload)

    def create_row(self, **data):
        return Row(self, None, **data)

    def atomic(self):
        return self.database.atomic()

    def all(self):
        query = (self.model
                 .select(
                     self.model.row_key,
                     self.model.column,
                     self.model.value)
                 .group_by(
                     self.model.row_key,
                     self.model.column)
                 .order_by(self.model.row_key, self.model.timestamp.desc())
                 .tuples())
        for row in row_iterator(self, query):
            yield row


class Row(object):
    def __init__(self, keyspace, identifier=None, preload=None, **data):
        self.keyspace = keyspace
        self.model = self.keyspace.model
        self.identifier = identifier
        self._data = data
        if preload:
            self.multi_get(preload)
        if self._data and not self.identifier:
            self.multi_set(self._data)

    def multi_get(self, columns):
        query = (self.model
                 .select(self.model.column, self.model.value)
                 .where(self.model.row_key == self.identifier)
                 .group_by(self.model.column)
                 .order_by(self.model.timestamp.desc())
                 .tuples())

        if columns is not True:
             query = query.where(self.model.column.in_(columns))

        data = {}
        for column, value in query:
            data[column] = value
            self._data[column] = value

        return data

    def multi_set(self, data):
        if not self.identifier:
            self.identifier = (self.model
                          .select(fn.COALESCE(
                              fn.MAX(self.model.row_key) + 1,
                              1))
                          .scalar())
        self.model.insert_many(rows=[
            {'column': key, 'value': value, 'row_key': self.identifier}
            for key, value in data.items()]).execute()

    def __setitem__(self, key, value):
        if self.identifier:
            row_key = self.identifier
        else:
            row_key = fn.COALESCE(
                self.model.select(fn.MAX(self.model.row_key) + 1),
                1)

        query = (self.model
                 .insert(
                     row_key=row_key,
                     column=key,
                     value=value,
                     timestamp=time.time())
                 .on_conflict('REPLACE'))
        rowid = query.execute()

        if not self.identifier:
            self.identifier = (self.model
                               .select(self.model.row_key)
                               .where(self.model.id == rowid)
                               .limit(1)
                               .scalar())
        self._data[key] = value

    def __getitem__(self, key):
        if key not in self._data:
            self._data[key] = (self.model
                               .select(self.model.value)
                               .where(
                                   (self.model.row_key == self.identifier) &
                                   (self.model.column == key))
                               .order_by(self.model.timestamp.desc())
                               .limit(1)
                               .scalar(convert=True))
        return self._data[key]

    def __delitem__(self, key):
        query = (self.model
                 .delete()
                 .where(
                     (self.model.row_key == self.identifier) &
                     (self.model.column == key)))
        query.execute()
        try:
            del self._data[key]
        except KeyError:
            pass

    def delete(self):
        return (self.model
                .delete()
                .where(self.model.row_key == self.identifier)
                .execute())

    def keys(self):
        if self.identifier and not self._data:
            self.multi_get(True)
        return self._data.keys()

    def values(self):
        if self.identifier and not self._data:
            self.multi_get(True)
        return self._data.values()

    def items(self):
        if self.identifier and not self._data:
            self.multi_get(True)
        return self._data.items()
