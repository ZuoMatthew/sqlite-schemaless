"""
SQLite-Schemaless

Data is stored in a single SQLite database, which can be either on-disk or
in-memory.
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
            row_key = IntegerField(index=True)
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
            'DELETE FROM %(index)s '
            'WHERE row_key = new.row_key; '
            'INSERT INTO %(index)s (row_key, value) '
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

    def query(self, value, operation=operator.eq):
        # Support string operations in addition to functional, for readability.
        if isinstance(operation, basestring):
            operation = self._op_map[operation]

        model = self.keyspace.model
        query = (model
                 .select(
                     model.row_key,
                     model.column,
                     model.value)
                 .join(
                     self.model,
                     on=(self.model.row_key == model.row_key))
                 .where(operation(self.model.value, value))
                 .group_by(
                     model.row_key,
                     model.column)
                 .order_by(model.row_key, model.timestamp.desc())
                 .tuples())

        for row in row_iterator(self.keyspace, query):
            yield row

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

        class Meta:
            db_table = self.db_table

        return type(self.name, (BaseModel,), {'Meta': Meta})

    def create(self):
        self.model.create_table()
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
        with self.keyspace.atomic():
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

        model = self.model.create(
            row_key=row_key,
            column=key,
            value=value)
        if not self.identifier:
            self.identifier = (self.model
                               .select(self.model.row_key)
                               .where(self.model.id == model.id)
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
