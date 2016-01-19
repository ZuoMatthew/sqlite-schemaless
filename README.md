sqlite-schemaless
=================

Schemaless database built on top of SQLite. I based the design of `sqlite-schemaless` on the data-store described in [this Uber engineering post](https://eng.uber.com/schemaless-part-one/). All data is stored in a single SQLite database, which can either be on-disk or in-memory.

Data is organized by **KeySpace**. A KeySpace might be something like "users" or "tweets". Inside each KeySpace there are **Rows**. A row is identified by an integer `row_key` and consists of one or more named columns. In these columns you can store arbitrary JSON blobs. So:

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

So far this is really not that interesting. Things get interesting, though, with the addition of **secondary indexes** and **event emitters**.

Secondary Indexes
-----------------

You can create indexes on values stored in the JSON column data. Suppose we are storing User data in the `user` column. This user data is structured roughly like this:

```javascript
{
  'name': 'Charles',
  'username': 'coleifer',
  'location': {
    'state': 'KS',
    'city': 'Lawrence',
  }
}
```

If we wanted to search for users by username, we would create an index using JSON-path notation: `$.username`. If we wanted to search by the user's state, we would create an index on `$.location.state`.

Here is how the code would look to set this up:

```python

db = Schemaless(':memory:')  # Create an in-memory database.

# Indexes are initialized with the column name and JSON-path.
username_idx = Index('user', '$.username')
state_idx = Index('user', '$.location.state')

# When we create a KeySpace, we can include a list of indexes:
users = db.keyspace('users', username_idx, state_idx)

# To store a user, write:
charles = users.create_row(
    user={
        'username': 'coleifer',
        'location': {'city': 'Lawrence', 'state': 'KS'},
    },
    social=[
        {'name': 'github', 'username': 'coleifer'},
        {'name': 'twitter', 'username': 'coleifer'},
    ])

# To query for users in Kansas, we can write:
ks_users = state_idx.query('KS')
for row in ks_users:
    print row['user']['username']
```

Event emitters
--------------

The other neat feature of `sqlite-schemaless` is the event emitter functionality. This feature allows you to bind handlers to execute code when a new row is created or updated. You can bind handlers to an individual keyspace, or to all keyspaces.

For example, let's add a simple handler to print usernames as they're added or updated.

```python

@users.handler
def print_username(row, column, value):
    if column == 'user':
        print value['username']
```

Whenever we add or update the `user` column of a row in the `users` KeySpace, the callback will fire and print the username.
