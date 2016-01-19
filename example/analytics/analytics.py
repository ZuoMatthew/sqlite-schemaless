#!/usr/bin/env python

from base64 import b64decode
import json
import os
import time
from urlparse import parse_qsl, urlparse

from flask import Flask, Response, abort, request
from schemaless import Index, Schemaless

# 1 pixel GIF, base64-encoded.
BEACON = b64decode('R0lGODlhAQABAIAAANvf7wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==')

# Store the database file in the app directory.
APP_DIR = os.path.realpath(os.path.dirname(__file__))
DATABASE_FILE = os.path.join(APP_DIR, 'analytics.db')
DOMAIN = 'http://127.0.0.1:5000'  # TODO: change me.

# Simple JavaScript which will be included and executed on the client-side.
JAVASCRIPT = """(function(){
    var d=document,i=new Image,e=encodeURIComponent;
    i.src='%s/a.gif?url='+e(d.location.href)+'&ref='+e(d.referrer)+'&t='+e(d.title);
    })()""".replace('\n', '')

# Flask application settings.
DEBUG = bool(os.environ.get('DEBUG'))
SECRET_KEY = 'sssh! its a secret!'

app = Flask(__name__)
app.config.from_object(__name__)

# Create our Schemaless database.
database = Schemaless(DATABASE_FILE)

# Define indexes on the `pageview` column.
url_index = Index('pageview', '$.url')
timestamp_index = Index('pageview', '$.timestamp')
referer_index = Index('pageview', '$.Referer')

# Define indexes on the `headers` column.
language_index = Index('headers', '$.Accept-Language')
user_agent_index = Index('headers', '$.User-Agent')

# Store page-views in a keyspace with the above indexes.
PageView = database.keyspace(
    'pageviews',
    url_index,
    timestamp_index,
    language_index,
    referer_index)

# Create a separate keyspace to store a mapping of referer host to URL.
# This keyspace an index on each of the two values we are storing, the
# referer's host and the URL that was visited.
referer_host_index = Index('data', '$.referer_host')
rh_url_index = Index('data', '$.url')

# Store referer-host to URL mapping in a separate keyspace.
RefererHost = database.keyspace(
    'referer_host',
    referer_host_index,
    rh_url_index)


@app.route('/a.gif')
def analyze():
    if not request.args.get('url'):
        abort(404)

    # Parse the URL.
    parsed = urlparse(request.args['url'])

    # Each PageView row consists of 3 columns:
    # 1. pageview: generic data about the pageview.
    # 2. headers: request headers sent by user's browser.
    # 3. query: query-string parameters.
    PageView.create_row(
        pageview={
            'ip': request.remote_addr,
            'url': parsed.path,
            'title': request.args['t'],
            'referer': request.args['ref'],
            'timestamp': time.time()},
        headers=dict(request.headers),
        query=dict(parse_qsl(parsed.query)))

    response = Response(app.config['BEACON'], mimetype='image/gif')
    response.headers['Cache-Control'] = 'private, no-cache'
    return response

@app.route('/a.js')
def script():
    return Response(
        app.config['JAVASCRIPT'] % (app.config['DOMAIN']),
        mimetype='text/javascript')

@app.errorhandler(404)
def not_found(e):
    return Response('Not found.')

# Here we will create an event handler so that whenver a pageview is created,
# we will also store a mapping of the referer's host to the URL that was
# visited.
@PageView.handler
def store_referer_host(row_key, column, value):
    if column == 'pageview' and value['referer']:
        parsed = urlparse(value['referer'])
        RefererHost.create_row(data={
            'referer_host': parsed.netloc,
            'url': value['url']})


if __name__ == '__main__':
    # Create tables, indexes, triggers.
    PageView.create()
    RefererHost.create()

    # Run locally using werkzeug WSGI server.
    try:
        app.run()
    finally:
        database.close()  # Close database on exit.
