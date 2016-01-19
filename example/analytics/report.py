#!/usr/bin/env python

"""
This is just a demonstration script that performs a handful of queries on
the analytics database.
"""

from analytics import PageView, RefererHost
from analytics import url_index


# Print all URLs viewed in the ordered in which they were viewed.
print 'All URLs:'
for pageview in PageView.all():
    print pageview['pageview']['url']


# Query for URLs that contained "sqlite" in the path and print the
# corresponding title.
print
print 'URLs containing "sqlite":'
for pageview in url_index.query('%sqlite%', 'LIKE'):
    print pageview['pageview']['title']


# Query for the referer hostnames and list the URLs visited.
print
print 'Referer host and URL:'
for referer_host in RefererHost.all():
    print referer_host['data']['referer_host'], referer_host['data']['url']
