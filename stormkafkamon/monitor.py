#!/usr/bin/env python

import argparse
import sys
from prettytable import PrettyTable
import requests
import simplejson as json
import datetime

from zkclient import ZkClient, ZkError
from processor import process, ProcessorError

def sizeof_fmt(num):
    for x in [' bytes','KB','MB','GB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')

def null_fmt(num):
    return num

def time_fmt(d):
    attrs = ['years', 'months', 'days', 'hours', 'minutes', 'seconds']
    human_readable = lambda delta: ['%d %s' % (getattr(delta, attr), getattr(delta, attr) > 1 and attr or attr[:-1]) for attr in attrs if getattr(delta, attr)]
    return human_readable(d) or ['0 seconds']

def display(summary, friendly=False, include_lag=False):
    if friendly:
        fmt = sizeof_fmt
    else:
        fmt = null_fmt

    headers = ['Broker', 'Topic', 'Partition', 'Earliest', 'Latest', 'Depth', 'Spout', 'Current', 'Delta']
    if include_lag:
        headers.extend(['Timestamp', 'Lag'])
    table = PrettyTable(headers)
    table.align['broker'] = 'l'

    for p in summary.partitions:
        fields = [p.broker, p.topic, p.partition, p.earliest, p.latest, fmt(p.depth), p.spout, p.current, fmt(p.delta)]
        if include_lag:
            fields.extend([datetime.datetime.fromtimestamp(p.timestamp/1000.0), time_fmt(p.lag)[0]])
        table.add_row(fields)
    print table.get_string(sortby='Broker')
    print
    print 'Number of brokers:       %d' % summary.num_brokers
    print 'Number of partitions:    %d' % summary.num_partitions
    print 'Total broker depth:      %s' % fmt(summary.total_depth)
    print 'Total delta:             %s' % fmt(summary.total_delta)
    print 'Total lag:               %s' % time_fmt(summary.total_lag)[0]

def post_json(endpoint, zk_data):
    fields = ("broker", "topic", "partition", "earliest", "latest", "depth",
              "spout", "current", "delta")
    json_data = {"%s-%s" % (p.broker, p.partition):
                 {name: getattr(p, name) for name in fields}
                 for p in zk_data.partitions}
    total_fields = ('depth', 'delta')
    total = {fieldname:
             sum(getattr(p, fieldname) for p in zk_data.partitions)
             for fieldname in total_fields}
    total['partitions'] = len({p.partition for p in zk_data.partitions})
    total['brokers'] = len({p.broker for p in zk_data.partitions})
    json_data['total'] = total
    print json.dumps(json_data)
    #requests.post(endpoint, data=json.dumps(json_data))

######################################################################

def true_or_false_option(option):
    if option == None:
        return False
    else:
        return True

def read_args():
    parser = argparse.ArgumentParser(
        description='Show complete state of Storm-Kafka consumers')
    parser.add_argument('--zserver', default='localhost',
        help='Zookeeper host (default: localhost)')
    parser.add_argument('--zport', type=int, default=2181,
        help='Zookeeper port (default: 2181)')
    parser.add_argument('--topology', type=str, required=True,
        help='Storm Topology')
    parser.add_argument('--spoutroot', type=str, required=True,
        help='Root path for Kafka Spout data in Zookeeper')
    parser.add_argument('--friendly', action='store_const', const=True,
        help='Show friendlier data')
    parser.add_argument('--postjson', type=str,
        help='endpoint to post json data to')
    parser.add_argument('--field_name', type=str,
        help='The name of the field containing the timestamp in the Kafka JSON message')
    parser.add_argument('--in_array', type=bool, default=False,
        help='Indicate that the Kafka messages are wrapped in an array')
    return parser.parse_args()

def main():
    options = read_args()

    zc = ZkClient(options.zserver, options.zport)

    try:
        zk_data = process(zc.spouts(options.spoutroot, options.topology),
                          field_name=options.field_name, in_array=options.in_array)
    except ZkError, e:
        print 'Failed to access Zookeeper: %s' % str(e)
        return 1
    except ProcessorError, e:
        print 'Failed to process: %s' % str(e)
        return 1
    else:
        if options.postjson:
            post_json(options.postjson, zk_data)
        else:
            display(zk_data, true_or_false_option(options.friendly), options.field_name != None)

    return 0

if __name__ == '__main__':
    sys.exit(main())
