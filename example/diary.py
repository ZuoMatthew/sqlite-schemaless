#!/usr/bin/env python

import datetime
import sys
from collections import OrderedDict

from schemaless import *


db = Schemaless('diary.db')

content_idx = Index('entry', '$.content')
timestamp_idx = Index('entry', '$.timestamp')
entries = db.keyspace('entries', content_idx, timestamp_idx)

def menu_loop():
    choice = None
    while choice != 'q':
        for key, value in menu.items():
            print('%s) %s' % (key, value.__doc__))
        choice = raw_input('Action: ').lower().strip()
        if choice in menu:
            menu[choice]()

def add_entry():
    """Add entry"""
    print('Enter your entry. Press ctrl+d when finished.')
    data = sys.stdin.read().strip()
    if data and raw_input('Save entry? [Yn] ') != 'n':
        entries.create_row(entry={
            'content': data,
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        })
        print('Saved successfully.')

def view_entries(search_query=None):
    """View previous entries"""
    if search_query:
        rows = content_idx.query('%%%s%%' % search_query, 'LIKE')
    else:
        rows = entries.all()

    for row in rows:
        dt_obj = datetime.datetime.strptime(
            row['entry']['timestamp'],
            '%Y-%m-%d %H:%M:%S')
        dt_display = dt_obj.strftime('%A %B %d, %Y %I:%M%p')
        print(dt_display)
        print('=' * len(dt_display))
        print(row['entry']['content'])
        print('n) next entry')
        print('d) delete entry')
        print('q) return to main menu')
        choice = raw_input('Choice? (Ndq) ').lower().strip()
        if choice == 'q':
            break
        elif choice == 'd':
            row.delete()
            print('Entry deleted successfully.')
            break

def search_entries():
    """Search entries"""
    view_entries(raw_input('Search query: '))

menu = OrderedDict([
    ('a', add_entry),
    ('v', view_entries),
    ('s', search_entries),
])

if __name__ == '__main__':
    entries.create()
    menu_loop()
