#!/usr/bin/env python
# -*- coding: utf-8 -*-

__path__ = ['..']

# worldclock.py show time of countries in the world.
# This is just a simple demo on how to use tabmon.py.

from datetime import datetime
from datetime import tzinfo
from datetime import timedelta
import time
import tabmon


class GMT8(tzinfo):
    def dst(self, dt):
        return timedelta(0)

    def fromutc(self, utc):
        return utc + timedelta(hours=8)

    def tzname(self, dt):
        return 'GMT+8'

    def utcoffset(self, dt):
        return timedelta(hours=8)


class GMT1(tzinfo):
    def dst(self, dt):
        return timedelta(0)

    def fromutc(self, utc):
        return utc + timedelta(hours=1)

    def tzname(self, dt):
        return "GMT+1"

    def utcoffset(self, dt):
        return timedelta(hours=8)


def get_times():
    beijing = GMT8()
    paris = GMT1()
    now = lambda tz: datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    rowset = []
    rowset.append({'CITY': 'Beijing', 'TIME': now(beijing)})
    rowset.append({'CITY': 'Paris', 'TIME': now(paris)})
    return rowset

if __name__ == '__main__':
    mon = tabmon.TabularMonitor()
    mon.add_col('CITY')
    mon.add_col('TIME')
    try:
        while True:
            rowset = get_times()
            mon.update(rowset)
            time.sleep(1)
    finally:
        mon.close()
