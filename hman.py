#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import os.path
import sys
import time
import requests
from tabmon import TabularMonitor
from metrics import RegionServerMetrics
import conf


def get_conf(name, default=None):
    if not hasattr(conf, name):
        return default
    return getattr(conf, name)


def get_slaves(srv_file):
    slaves = []
    with open(srv_file, 'r') as sf:
        for line in sf:
            if line.strip()[0:1] == '#':
                # comment, skip it
                continue
            else:
                slaves.append(line.strip())
    return slaves


def get_hbase_conf(hmaster_info_addr):
    try:
        conf_url = '%s/conf?format=json' % (hmaster_info_addr,)
        r = requests.get(conf_url)
        if r.status_code == 404:
            # no /conf ? OMG
            pass
        r.raise_for_status()

        hbase_conf = {}
        for item in r.json()['properties']:
            hbase_conf[item['key']] = item
        return hbase_conf
    except requests.exceptions.ConnectionError:
        return None


if __name__ == '__main__':
    hbase_home = get_conf('HBASE_HOME', os.getenv('HBASE_HOME'))
    if hbase_home is None:
        sys.stderr.write('No HBASE_HOME is set, set it in conf.py or as a eviroment variable.\n')
        sys.exit(1)
    hmaster_info_addr = get_conf('HMASTER_INFO_ADDR')
    if hmaster_info_addr is None:
        sys.stderr.write('No HMASTER_INFO_ADDR is set, set it in conf.py\n')
        sys.exit(1)
    update_interval = get_conf('INTERVAL', 10)
    if update_interval is None:
        sys.stderr.write('No INTERVAL is set, set it in conf.py\n')
        sys.exit(1)

    srv_file = os.path.join(hbase_home, 'conf/regionservers')
    if not os.path.exists(srv_file):
        sys.stderr.write(
            'Regionservers file ' + srv_file + ' not exists.\n')
        sys.exit(1)
    regionservers = get_slaves(srv_file)
    if len(regionservers) == 0:
        # no region servers
        sys.stderr.write('No regionservers specified.\n')
        sys.exit(1)

    hbase_conf = get_hbase_conf(hmaster_info_addr)
    if hbase_conf is None:
        sys.stderr.write('Cannot get configuration from HMaster info address\n')
        sys.exit(1)
    rs_info_port = int(hbase_conf['hbase.regionserver.info.port']['value'])
    rs_metrics = []
    for rs in regionservers:
        rs_metrics.append(RegionServerMetrics(rs, rs_info_port))

    # main loop
    mon = TabularMonitor()
    for key in RegionServerMetrics.keys:
        mon.add_col(key)

    try:
        while True:
            for m in rs_metrics:
                # should make it parallel
                m.poll()
            mon.update(rs_metrics)
            time.sleep(update_interval)
    finally:
        mon.close()
