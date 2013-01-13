# -*- coding: utf-8 -*-

from collections import deque
import requests


class MetricsItem(object):
    '''A single metrics item.'''

    def __init__(self, name, func, requried=1, formatter=None):
        self.name = name
        self.func = func
        self.formatter = formatter
        self.requried = requried
        self.snapshots = None

    def update(self, snapshots):
        self.snapshots = snapshots

    def __str__(self):
        if len(self.snapshots) < self.requried:
            return 'WAITING'
        val = self.func(self.snapshots)
        if self.formatter is not None:
            val = self.formatter(val)
        return str(val)


class RegionServerMetrics(object):
    '''Represent metrics for a region server.'''

    keys = []
    items = {}
    max_snapshots = 0

    @classmethod
    def register(cls, name, func, requried=1, formatter=None):
        cls.keys.append(name)
        cls.items[name] = MetricsItem(name, func, requried, formatter)
        cls.max_snapshots = max(requried, cls.max_snapshots)

    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port
        self.snapshots = deque(maxlen=self.max_snapshots)
        self.is_alive = True

    def poll(self):
        '''Poll metrics from region server.'''
        metrics = self._get_metrics()
        self.snapshots.appendleft(metrics)

    def _get_metrics(self):
        '''Get metrics from regionserver rs.

        Return metrics as JSON.'''
        try:
            metrics_url = 'http://%s:%d/metrics?format=json' \
                    % (self.hostname, self.port)
            r = requests.get(metrics_url)
            if r.status_code == 400:
                # TODO: oops! no /metrics ?
                pass
            r.raise_for_status()
            # can be connected to, is alive or back into live again
            self.is_alive = True
            return r.json()
        except requests.exceptions.ConnectionError:
            self.is_alive = False
            return None

    def __getitem__(self, key):
        if not self.is_alive:
            return 'DEAD'
        if key not in self.keys:
            raise KeyError()
        self.items[key].update(self.snapshots)
        return str(self.items[key])


def _delta(valfunc):
    return lambda pair: valfunc(pair[0]) - valfunc(pair[1])

# register known metrics
RegionServerMetrics.register(
    'HOSTNAME',
    lambda xs: xs[0]['rpc']['metrics'][0][0]['hostName'])
RegionServerMetrics.register(
    'RPC_MULTIS',
    _delta(lambda x: x['rpc']['metrics'][0][1]['multi_num_ops']),
    requried=2)
RegionServerMetrics.register(
    'REQS/S',
    lambda xs: xs[0]['hbase']['regionserver'][0][1]['requests'],
    formatter=int)
RegionServerMetrics.register(
    'FLU_SIZE',
    lambda xs: xs[0]['hbase']['regionserver'][0][1]['flushSize_avg_time'])
RegionServerMetrics.register(
    'RGNS',
    lambda xs: xs[0]['hbase']['regionserver'][0][1]['regions'])
RegionServerMetrics.register(
    'STR_FILES',
    lambda xs: xs[0]['hbase']['regionserver'][0][1]['storefiles'])
RegionServerMetrics.register(
    'CMPCT_Q_SZ',
    lambda xs: xs[0]['hbase']['regionserver'][0][1]['compactionQueueSize'])
