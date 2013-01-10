#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import os.path
import sys
import time
import requests
import tabmon


class HBaseMetrics(object):
    '''Represent a hbase metrics.'''

    DELTA_METRICS = 0
    GAUGE_METRICS = 1

    METRICS_KEYS = []
    METRICS_KEYS.append('HOSTNAME')
    METRICS_KEYS.append('MULTI_OPS')
    METRICS_KEYS.append('MEMSTORE')
    METRICS_KEYS.append('REQS')
    METRICS_KEYS.append('FLU_SIZE')

    METRICS_TYPES = {}
    METRICS_TYPES['HOSTNAME'] = GAUGE_METRICS
    METRICS_TYPES['MULTI_OPS'] = DELTA_METRICS
    METRICS_TYPES['MEMSTORE'] = GAUGE_METRICS
    METRICS_TYPES['REQS'] = GAUGE_METRICS
    METRICS_TYPES['FLU_SIZE'] = GAUGE_METRICS

    METRICS_FUNCS = {}
    METRICS_FUNCS['HOSTNAME'] = lambda x: x['rpc']['metrics'][0][0]['hostName']
    METRICS_FUNCS['MULTI_OPS'] = lambda x: x['rpc']['metrics'][0][1]['multi_num_ops']
    METRICS_FUNCS['MEMSTORE'] = lambda x: x['hbase']['regionserver'][0][1]['memstoreSizeMB']
    METRICS_FUNCS['REQS'] = lambda x: x['hbase']['regionserver'][0][1]['requests']
    METRICS_FUNCS['FLU_SIZE'] = lambda x: x['hbase']['regionserver'][0]['flushSize_avg_time']

    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port
        self.snapshots = []

    def poll(self):
        '''Poll metrics from region server.'''
        metrics = self._get_metrics()
        if len(self.snapshots) < 2:
            self.snapshots.append(metrics)
        else:
            self.snapshots[0], self.snapshots[1] = self.snapshots[1], metrics

    def _get_metrics(self):
        '''Get metrics from regionserver rs.

        Return metrics as JSON.'''
        metrics_url = 'http://%s:%d/metrics?format=json' % (self.hostname, self.port)
        r = requests.get(metrics_url)
        r.raise_for_status()
        return r.json()

    def __getitem__(self, key):
        if key not in self.METRICS_KEYS:
            raise KeyError()
        if self.METRICS_TYPES[key] == self.GAUGE_METRICS:
            return str(self._get_gauge(key))
        elif self.METRICS_TYPES[key] == self.DELTA_METRICS:
            return str(self._get_delta(key))

    def _get_delta(self, key):
        if self.is_avaible():
            func = self.METRICS_FUNCS[key]
            last_multi = func(self.snapshots[0])
            curr_multi = func(self.snapshots[1])
            return curr_multi - last_multi

    def _get_gauge(self, key):
        if self.is_avaible():
            func = self.METRICS_FUNCS[key]
            return func(self.snapshots[1])

    def is_avaible(self):
        return len(self.snapshots) == 2


if __name__ == '__main__':
    opt = argparse.ArgumentParser()
    opt.add_argument('--servers', help='regionservers file')
    opt.add_argument('--port', help='regionserver port', type=int)
    args = opt.parse_args()

    srv_file = None
    if args.servers:
        srv_file = args.servers
    else:
        # get from HBASE_HOME/conf
        hbase_home = os.getenv('HBASE_HOME')
        if hbase_home is None:
            sys.stderr.write('No regionservers file found.\n')
            sys.exit(1)
        srv_file = os.path.join(hbase_home, 'conf/regionservers')
        if not os.path.exists(srv_file):
            sys.stderr.write(
                'Regionservers file ' + srv_file + ' not exists.\n')
            sys.exit(1)

    port = 60030
    if args.port:
        port = args.port

    regionservers = []
    with open(srv_file, 'r') as sf:
        for line in sf:
            if line.strip()[0:1] == '#':
                # comment, skip it
                continue
            else:
                regionservers.append(line.strip())
    if len(regionservers) == 0:
        # no region servers
        sys.stderr.write('No regionservers specified.\n')
        sys.exit(1)

    hms = []
    for rs in regionservers:
        hms.append(HBaseMetrics(rs, port))
    mon = tabmon.TabularMonitor()
    for key in HBaseMetrics.METRICS_KEYS:
        mon.add_col(key)

    # main loop
    try:
        while True:
            for hm in hms:
                hm.poll()
            metrics = [m for m in hms if m.is_avaible()]
            mon.update(metrics)
            time.sleep(10)
    finally:
        mon.close()
