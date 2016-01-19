"""
SQLite-Schemaless

Data is stored in a single SQLite database, which can be either on-disk or
in-memory.
"""
import operator
import re
import sys
import time
import unittest
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



class TestKeySpace(unittest.TestCase):
    def setUp(self):
        self.db = Schemaless(':memory:')
        self.keyspace = self.db.keyspace('test-keyspace')
        self.keyspace.create()

    def tearDown(self):
        self.keyspace.drop()
        if not self.db.is_closed():
            self.db.close()

    def test_storage(self):
        charlie = self.keyspace.create_row()
        self.assertEqual(charlie.identifier, None)

        # First write will allocate an identifier.
        charlie['name'] = 'charlie'
        self.assertEqual(charlie.identifier, 1)

        # Write some more columns.
        charlie['eyes'] = 'brown'
        charlie['hair'] = 'dark brown'

        huey = self.keyspace.create_row()
        huey['name'] = 'huey'
        huey['eyes'] = 'blue'
        huey['fur'] = 'white'

        # Test retrieving data.
        c_db = self.keyspace[charlie.identifier]
        self.assertEqual(c_db['name'], 'charlie')
        self.assertEqual(c_db['eyes'], 'brown')
        self.assertEqual(c_db['hair'], 'dark brown')

        h_db = self.keyspace[huey.identifier]
        self.assertEqual(h_db['name'], 'huey')
        self.assertEqual(h_db['eyes'], 'blue')
        self.assertEqual(h_db['fur'], 'white')

        # Non-existant columns return NULL.
        self.assertIsNone(c_db['fur'])
        self.assertIsNone(h_db['hair'])

        # Overwriting values will pick the newest first. Since values are
        # cached, though, we will need to re-load from the database.
        huey['fur'] = 'white and gray'
        self.assertEqual(huey['fur'], 'white and gray')
        self.assertEqual(h_db['fur'], 'white')  # Old value.

        h_db = self.keyspace[huey.identifier]
        self.assertEqual(h_db['fur'], 'white and gray')

        del huey['fur']
        self.assertIsNone(huey['fur'])
        self.assertEqual(h_db['fur'], 'white and gray')  # Old value.

        h_db = self.keyspace[huey.identifier]
        self.assertIsNone(h_db['fur'])

    def test_all(self):
        for i in range(1, 5):
            self.keyspace.create_row(**{'k1': 'v1-%s' % i, 'k2': 'v2-%s' % i})

        rows = [row for row in self.keyspace.all()]
        self.assertEqual([row._data for row in rows], [
            {'k1': 'v1-1', 'k2': 'v2-1'},
            {'k1': 'v1-2', 'k2': 'v2-2'},
            {'k1': 'v1-3', 'k2': 'v2-3'},
            {'k1': 'v1-4', 'k2': 'v2-4'},
        ])

    def test_preload(self):
        r1 = self.keyspace.create_row()
        r2 = self.keyspace.create_row()
        r1['k1-1'] = 'v1'
        r1['k2-1'] = 'v2'
        r1['k3-1'] = 'v3'
        r2['k1-2'] = 'x1'
        r2['k2-2'] = 'x2'
        r2['k3-2'] = 'x3'

        r1_db = self.keyspace.get_row(r1.identifier, ('k1-1', 'k3-1'))
        self.assertEqual(r1_db._data, {
            'k1-1': 'v1',
            'k3-1': 'v3'})

        # Load other values.
        self.assertEqual(r1_db['k1-1'], 'v1')
        self.assertEqual(r1_db['k2-1'], 'v2')
        self.assertEqual(r1_db._data, {
            'k1-1': 'v1',
            'k2-1': 'v2',
            'k3-1': 'v3'})

        r2_db = self.keyspace.get_row(r2.identifier, (
            'k1-2',
            'k3-2',
            'kx',  # Non-existant column should not be loaded.
            'k2-1',  # Column from `r1` should not be loaded.
        ))
        self.assertEqual(r2_db._data, {
            'k1-2': 'x1',
            'k3-2': 'x3'})

        # Test loading all columns.
        r1_db2 = self.keyspace.get_row(r1.identifier, True)
        self.assertEqual(r1_db2._data, {
            'k1-1': 'v1',
            'k2-1': 'v2',
            'k3-1': 'v3'})

    def test_create_with_data(self):
        r1 = self.keyspace.create_row(k1_1='v1', k2_1='v2', k3_1='v3')
        r2 = self.keyspace.create_row(k1_2='x1', k2_2='x2', k3_2='x3')

        self.assertEqual(r1._data, {
            'k1_1': 'v1',
            'k2_1': 'v2',
            'k3_1': 'v3'})
        self.assertEqual(r2._data, {
            'k1_2': 'x1',
            'k2_2': 'x2',
            'k3_2': 'x3'})

        # Getting data works as expected.
        self.assertEqual(r1['k1_1'], 'v1')

        r1_db = self.keyspace[r1.identifier]
        self.assertEqual(r1_db['k1_1'], 'v1')
        self.assertEqual(r1_db['k2_1'], 'v2')
        self.assertEqual(r1_db['k3_1'], 'v3')

        # No values from row2.
        self.assertIsNone(r1_db['k1_2'])

    def test_index(self):
        username = Index('user', '$.user.username')
        pet_name = Index('user', '$.user.pets[0].name')
        keyspace = self.db.keyspace('test2', username, pet_name)
        keyspace.create()

        charlie = {
            'user': {
                'username': 'charlie',
                'active': True,
                'pets': [
                    {'name': 'huey', 'type': 'cat'},
                    {'name': 'mickey', 'type': 'dog'},
                ],
            },
        }
        nuggie = {
            'user': {
                'username': 'nuggie',
                'active': True,
                'pets': [
                    {'name': 'zaizee', 'type': 'cat'},
                    {'name': 'beanie', 'type': 'cat'},
                ],
            },
        }

        keyspace.create_row(user=charlie, misc={'foo': 'bar'})
        keyspace.create_row(user=nuggie, misc={'foo': 'baze'})

        rows = [row for row in username.query('charlie')]
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row._data, {
            'user': charlie,
            'misc': {'foo': 'bar'},
        })

        rows = [row for row in pet_name.query('zaizee')]
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row._data, {
            'user': nuggie,
            'misc': {'foo': 'baze'},
        })

    def populate_test_index(self):
        idx = Index('data', '$.k1')
        keyspace = self.db.keyspace('test3', idx)
        keyspace.create()

        keyspace.create_row(data={'k1': 'v1-1'}, misc=1337)
        keyspace.create_row(data={'k1': 'v1-2'})
        keyspace.create_row(data={'k1': 'v1-3'}, k1='v1-5', k2='v1-6')
        keyspace.create_row(data={'k1': 'xx'})
        keyspace.create_row(data={'k2': 'v1-x'})
        keyspace.create_row(data={'k1': 'v1-4', 'k2': 'v1-y'})
        return idx

    def test_multi_index(self):
        idx = self.populate_test_index()

        rows = [row for row in idx.query('v1-%', operator.pow)]
        self.assertEqual([row._data['data']['k1'] for row in rows], [
            'v1-1',
            'v1-2',
            'v1-3',
            'v1-4'])
        # All columns are fetched.
        self.assertEqual([row._data for row in rows], [
            {'data': {'k1': 'v1-1'}, 'misc': 1337},
            {'data': {'k1': 'v1-2'}},
            {'data': {'k1': 'v1-3'}, 'k1': 'v1-5', 'k2': 'v1-6'},
            {'data': {'k1': 'v1-4', 'k2': 'v1-y'}},
        ])

        rows = [row for row in idx.query('v1-x')]
        self.assertEqual(rows, [])

        rows = [row for row in idx.query('v1-2', operator.gt)]
        self.assertEqual([row._data['data']['k1'] for row in rows], [
            'v1-3',
            'xx',
            'v1-4'])

    def test_index_delete(self):
        idx = self.populate_test_index()
        keyspace = idx.keyspace

        all_items = [item for item in idx.all_items()]
        self.assertEqual(all_items, [
            {'row_key': 1, 'value': 'v1-1'},
            {'row_key': 2, 'value': 'v1-2'},
            {'row_key': 3, 'value': 'v1-3'},
            {'row_key': 4, 'value': 'xx'},
            {'row_key': 6, 'value': 'v1-4'},
        ])

        row = keyspace[2]
        row.delete()

        row = keyspace[4]
        row.delete()

        all_items = [item for item in idx.all_items()]
        self.assertEqual(all_items, [
            {'row_key': 1, 'value': 'v1-1'},
            {'row_key': 3, 'value': 'v1-3'},
            {'row_key': 6, 'value': 'v1-4'},
        ])

    def test_signal_handler(self):
        accum = []

        idx = Index('data', '$.k1')
        keyspace = self.db.keyspace('testing', idx)
        keyspace.create()

        @keyspace.handler
        def handler(row_key, column, value):
            accum.append((row_key, column, value))

        keyspace.create_row(data={'k1': 'v1'})
        keyspace.create_row(data={'k2': 'v2'})
        keyspace.create_row(data={'k3': 'v3'})
        self.assertEqual(accum, [
            (1, 'data', {'k1': 'v1'}),
            (2, 'data', {'k2': 'v2'}),
            (3, 'data', {'k3': 'v3'}),
        ])

        handler.unbind()
        keyspace.create_row(data={'k4': 'v4'})
        self.assertEqual(len(accum), 3)

    def test_json_extract_fallback(self):
        data = {
            'k1': 'v1',
            'k2': {
                'k3': ['v3', 'v4', {'k4': 'v5'}],
                'k5': ['v6', {'k6': ['v7', 'v8']}],
            },
        }
        json_data = json.dumps(data)

        def assertValue(path, expected):
            self.assertEqual(_json_extract_fallback(json_data, path), expected)

        assertValue('$.k1', 'v1')
        assertValue('$.k2.k3[0]', 'v3')
        assertValue('$.k2.k3[1]', 'v4')
        assertValue('$.k2.k3[2].k4', 'v5')
        assertValue('$.k2.k5[0]', 'v6')
        assertValue('$.k2.k5[1].k6[0]', 'v7')
        assertValue('$.k2.k5[1].k6[1]', 'v8')
        assertValue('$.kx', None)
        assertValue('$.kx[0]', None)
        assertValue('$.[0].kx', None)

        data = [[['foo'], 'bar'], 'baz', {'k1': ['v1']}]
        json_data = json.dumps(data)

        def assertValue(path, expected):
            self.assertEqual(_json_extract_fallback(json_data, path), expected)
        assertValue('$.[0][0][0]', 'foo')
        assertValue('$.[0][1]', 'bar')
        assertValue('$.[1]', 'baz')
        assertValue('$.[2].k1[0]', 'v1')


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
