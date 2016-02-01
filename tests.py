#!/usr/bin/env python

import json
import operator
import sys
import unittest

from schemaless import _json_extract_fallback
from schemaless import Index
from schemaless import Schemaless


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

    def test_upsert(self):
        Model = self.keyspace.model
        charlie = self.keyspace.create_row()

        # First write will allocate an identifier.
        charlie['name'] = 'charlie'
        self.assertEqual(charlie.identifier, 1)
        self.assertEqual(Model.select().count(), 1)

        charlie['country'] = 'US'
        charlie['state'] = 'KS'
        self.assertEqual(Model.select().count(), 3)

        # Test upsert.
        charlie['state'] = 'Kansas'
        self.assertEqual(Model.select().count(), 3)

        value = (Model
                 .select(Model.value)
                 .where(Model.column == 'state')
                 .scalar(convert=True))
        self.assertEqual(value, 'Kansas')

        for i in range(10):
            charlie['count'] = i

        self.assertEqual(Model.select().count(), 4)
        value = (Model
                 .select(Model.value)
                 .where(Model.column == 'count')
                 .scalar(convert=True))
        self.assertEqual(value, 9)

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

    def populate_test_index(self, *indexes):
        idx = Index('data', '$.k1')
        keyspace = self.db.keyspace('test3', idx, *indexes)
        keyspace.create()

        keyspace.create_row(data={'k1': 'v1-1'}, misc=1337)
        keyspace.create_row(data={'k1': 'v1-2', 'k2': 'x1-2'})
        keyspace.create_row(data={'k1': 'v1-3'}, k1='v1-5', k2='v1-6')
        keyspace.create_row(data={'k1': 'xx', 'k2': 'x1-xx'})
        keyspace.create_row(data={'k2': 'v1-x'})
        keyspace.create_row(data={'k1': 'v1-4', 'k2': 'v1-y'})
        return idx

    def test_query_expressions(self):
        idx2 = Index('data', '$.k2')
        idx = self.populate_test_index(idx2)

        query = idx.query('v1-1') | idx.query('v1-3') | idx.query('xx')
        self.assertEqual([row._data['data']['k1'] for row in query], [
            'v1-1',
            'v1-3',
            'xx'])

        query = (idx.query('v1-1') | idx.query('v1-3')) | idx2.query('x1-xx')
        self.assertEqual([row._data['data'] for row in query], [
            {'k1': 'v1-1'},
            {'k1': 'v1-3'},
            {'k1': 'xx', 'k2': 'x1-xx'},
        ])

    def test_query_descriptor(self):
        idx = self.populate_test_index()
        query = idx.query((idx.v == 'v1-1') | (idx.v == 'v1-3'))
        self.assertEqual([row._data for row in query], [
            {'data': {'k1': 'v1-1'}, 'misc': 1337},
            {'data': {'k1': 'v1-3'}, 'k1': 'v1-5', 'k2': 'v1-6'},
        ])

        query = idx.query((idx.v == 'v1-1') | (idx.v == 'v1-3'), reverse=True)
        self.assertEqual([row._data for row in query], [
            {'data': {'k1': 'v1-3'}, 'k1': 'v1-5', 'k2': 'v1-6'},
            {'data': {'k1': 'v1-1'}, 'misc': 1337},
        ])

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
            {'data': {'k1': 'v1-2', 'k2': 'x1-2'}},
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

        del keyspace[4]

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
        keyspace.create_row(data={'k3': 'v3', 'x3': 'y3'})
        self.assertEqual(accum, [
            (1, 'data', {'k1': 'v1'}),
            (2, 'data', {'k2': 'v2'}),
            (3, 'data', {'k3': 'v3', 'x3': 'y3'}),
        ])

        # Multiple columns correspond to multiple events.
        keyspace.create_row(col1='val1', col2='val2')
        self.assertEqual(len(accum), 5)
        col1, col2 = sorted(accum[3:])
        self.assertEqual(col1, (4, 'col1', 'val1'))
        self.assertEqual(col2, (4, 'col2', 'val2'))

        # After unbinding, no more events.
        handler.unbind()
        keyspace.create_row(data={'k4': 'v4'})
        self.assertEqual(len(accum), 5)

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
