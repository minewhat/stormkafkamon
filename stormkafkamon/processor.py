# Takes lists of objects returned by the zkclient module, and
# consolidates the information for display.

import logging
import simplejson as json
from datetime import datetime
from dateutil.relativedelta import relativedelta

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logger = logging.getLogger('kafka.codec').addHandler(NullHandler())

import socket
from collections import namedtuple
from kafka.client import KafkaClient
from kafka.common import OffsetRequest, FetchRequest


class ProcessorError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

PartitionState = namedtuple('PartitionState',
    [
        'broker',           # Broker host
        'topic',            # Topic on broker
        'partition',        # The partition
        'earliest',         # Earliest offset within partition on broker
        'latest',           # Current offset within partition on broker
        'depth',            # Depth of partition on broker.
        'spout',            # The Spout consuming this partition
        'current',          # Current offset for Spout
        'delta',            # Difference between latest and current
        'timestamp',        # Current offset datetime
        'lag',              # Time lag between Storm and Kafka latest
    ])
PartitionsSummary = namedtuple('PartitionsSummary',
    [
        'total_depth',      # Total queue depth.
        'total_delta',      # Total delta across all spout tasks.
        'total_lag',        # Total lag between Storm and Kafka
        'num_partitions',   # Number of partitions.
        'num_brokers',      # Number of Kafka Brokers.
        'partitions'        # Tuple of PartitionStates
    ])

def get_timestamp(k, p, current):
    buffer_size = 1024
    responses = k.send_fetch_request([FetchRequest(p['topic'], p['partition'], current, buffer_size)])
    for resp in responses:
        for message in resp.messages:
            return json.loads(message.message.value)['s']

def now():
    return int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)

def max_relativedate(a, b):
    attrs = ['years', 'months', 'days', 'hours', 'minutes', 'seconds']
    for attr in attrs:
        if getattr(a, attr) > getattr(b, attr):
            return a
        if getattr(b, attr) > getattr(a, attr):
            return b
    return a

def process(spouts):
    '''
    Returns a named tuple of type PartitionsSummary.
    '''
    results = []
    total_depth = 0
    total_delta = 0
    total_lag = relativedelta()
    brokers = []
    seen_hosts, seen_topics, seen_partitions = set(), set(), set()
    for s in spouts:
        for p in s.partitions:
            if p['broker']['host'] in seen_hosts and p['topic'] in seen_topics and p['partition'] in seen_partitions:
                continue
            else:
                seen_hosts.add(p['broker']['host'])
                seen_topics.add(p['topic'])
                seen_partitions.add(p['partition'])

            try:
                k = KafkaClient('%s:%s' % (p['broker']['host'], p['broker']['port']))
            except socket.gaierror, e:
                raise ProcessorError('Failed to contact Kafka broker %s (%s)' %
                                     (p['broker']['host'], str(e)))
            earliest_off = OffsetRequest(p['topic'], p['partition'], -2, 1)
            latest_off = OffsetRequest(p['topic'], p['partition'], -1, 1)

            earliest = k.send_offset_request([earliest_off])[0].offsets[0]
            latest = k.send_offset_request([latest_off])[0].offsets[0]
            current = p['offset']

            brokers.append(p['broker']['host'])
            total_depth = total_depth + (latest - earliest)
            total_delta = total_delta + (latest - current)

            timestamp = get_timestamp(k, p, current) or 0
            lag = relativedelta(datetime.now(), datetime.fromtimestamp(timestamp/1000.0))
            total_lag = max_relativedate(total_lag, lag)

            results.append(PartitionState._make([
                p['broker']['host'],
                p['topic'],
                p['partition'],
                earliest,
                latest,
                latest - earliest,
                s.id,
                current,
                latest - current,
                timestamp,
                lag]))
    return PartitionsSummary(total_depth=total_depth,
                             total_delta=total_delta,
                             total_lag=total_lag,
                             num_partitions=len(results),
                             num_brokers=len(set(brokers)),
                             partitions=tuple(results))
