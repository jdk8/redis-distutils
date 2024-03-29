# -*- coding: utf-8 -*-
from socket import gethostbyaddr
from functools import wraps
import time

# rediscluster imports
from .exceptions import (
    RedisClusterException, ClusterDownError
)

# 3rd party imports
from redis._compat import basestring, nativestr


class DistLock(object):

    def __init__(self, unique_id, lock_valid_time, begin_lock_time, is_get_lock, lock_keys):
        super(DistLock, self).__init__()
        self.unique_id = unique_id
        self.lock_valid_time = lock_valid_time
        self.begin_lock_time = begin_lock_time
        self.is_get_lock = is_get_lock
        self.lock_keys = lock_keys

    def is_lock_valid(self):
        current_time = int(time.time())
        return self.is_get_lock and (current_time - self.begin_lock_time < self.lock_valid_time)


def bool_ok(response, *args, **kwargs):
    """
    Borrowed from redis._compat becuase that method to not support extra arguments
    when used in a cluster environment.
    """
    return nativestr(response) == 'OK'


def string_keys_to_dict(key_strings, callback):
    """
    Maps each string in `key_strings` to `callback` function
    and return as a dict.
    """
    return dict.fromkeys(key_strings, callback)


def dict_merge(*dicts):
    """
    Merge all provided dicts into 1 dict.
    """
    merged = {}

    for d in dicts:
        if not isinstance(d, dict):
            raise ValueError('Value should be of dict type')
        else:
            merged.update(d)

    return merged


def blocked_command(self, command):
    """
    Raises a `RedisClusterException` mentioning the command is blocked.
    """
    raise RedisClusterException("Command: {0} is blocked in redis cluster mode".format(command))


def merge_result(command, res):
    """
    Merge all items in `res` into a list.

    This command is used when sending a command to multiple nodes
    and they result from each node should be merged into a single list.
    """
    if not isinstance(res, dict):
        raise ValueError('Value should be of dict type')

    result = set([])

    for _, v in res.items():
        for value in v:
            result.add(value)

    return list(result)


def first_key(command, res):
    """
    Returns the first result for the given command.

    If more then 1 result is returned then a `RedisClusterException` is raised.
    """
    if not isinstance(res, dict):
        raise ValueError('Value should be of dict type')

    if len(res.keys()) != 1:
        raise RedisClusterException("More then 1 result from command: {0}".format(command))

    return list(res.values())[0]


def clusterdown_wrapper(func):
    """
    Wrapper for CLUSTERDOWN error handling.

    If the cluster reports it is down it is assumed that:
     - connection_pool was disconnected
     - connection_pool was reseted
     - refereh_table_asap set to True

    It will try 3 times to rerun the command and raises ClusterDownException if it continues to fail.
    """
    @wraps(func)
    def inner(*args, **kwargs):
        for _ in range(0, 3):
            try:
                return func(*args, **kwargs)
            except ClusterDownError:
                # Try again with the new cluster setup. All other errors
                # should be raised.
                pass

        # If it fails 3 times then raise exception back to caller
        raise ClusterDownError("CLUSTERDOWN error. Unable to rebuild the cluster")
    return inner


def nslookup(node_ip):
    """
    """
    if ':' not in node_ip:
        return gethostbyaddr(node_ip)[0]

    ip, port = node_ip.split(':')

    return '{0}:{1}'.format(gethostbyaddr(ip)[0], port)


def parse_cluster_slots(resp, **options):
    """
    """
    current_host = options.get('current_host', '')

    def fix_server(*args):
        return (nativestr(args[0]) or current_host, args[1])

    slots = {}
    for slot in resp:
        start, end, master = slot[:3]
        slaves = slot[3:]
        slots[start, end] = {
            'master': fix_server(*master),
            'slaves': [fix_server(*slave) for slave in slaves],
        }

    return slots


def parse_cluster_nodes(resp, **options):
    """
    @see: http://redis.io/commands/cluster-nodes  # string
    @see: http://redis.io/commands/cluster-slaves # list of string
    """
    resp = nativestr(resp)
    current_host = options.get('current_host', '')

    def parse_slots(s):
        slots, migrations = [], []
        for r in s.split(' '):
            if '->-' in r:
                slot_id, dst_node_id = r[1:-1].split('->-', 1)
                migrations.append({
                    'slot': int(slot_id),
                    'node_id': dst_node_id,
                    'state': 'migrating'
                })
            elif '-<-' in r:
                slot_id, src_node_id = r[1:-1].split('-<-', 1)
                migrations.append({
                    'slot': int(slot_id),
                    'node_id': src_node_id,
                    'state': 'importing'
                })
            elif '-' in r:
                start, end = r.split('-')
                slots.extend(range(int(start), int(end) + 1))
            else:
                slots.append(int(r))

        return slots, migrations

    if isinstance(resp, basestring):
        resp = resp.splitlines()

    nodes = []
    for line in resp:
        parts = line.split(' ', 8)
        self_id, addr, flags, master_id, ping_sent, \
            pong_recv, config_epoch, link_state = parts[:8]

        host, ports = addr.rsplit(':', 1)
        port, _, cluster_port = ports.partition('@')

        node = {
            'id': self_id,
            'host': host or current_host,
            'port': int(port),
            'cluster-bus-port': int(cluster_port) if cluster_port else 10000 + int(port),
            'flags': tuple(flags.split(',')),
            'master': master_id if master_id != '-' else None,
            'ping-sent': int(ping_sent),
            'pong-recv': int(pong_recv),
            'link-state': link_state,
            'slots': [],
            'migrations': [],
        }

        if len(parts) >= 9:
            slots, migrations = parse_slots(parts[8])
            node['slots'], node['migrations'] = tuple(slots), migrations

        nodes.append(node)

    return nodes


def parse_pubsub_channels(command, res, **options):
    """
    Result callback, handles different return types
    switchable by the `aggregate` flag.
    """
    aggregate = options.get('aggregate', True)
    if not aggregate:
        return res
    return merge_result(command, res)


def parse_pubsub_numpat(command, res, **options):
    """
    Result callback, handles different return types
    switchable by the `aggregate` flag.
    """
    aggregate = options.get('aggregate', True)
    if not aggregate:
        return res

    numpat = 0
    for node, node_numpat in res.items():
        numpat += node_numpat
    return numpat


def parse_pubsub_numsub(command, res, **options):
    """
    Result callback, handles different return types
    switchable by the `aggregate` flag.
    """
    aggregate = options.get('aggregate', True)
    if not aggregate:
        return res

    numsub_d = dict()
    for _, numsub_tups in res.items():
        for channel, numsubbed in numsub_tups:
            try:
                numsub_d[channel] += numsubbed
            except KeyError:
                numsub_d[channel] = numsubbed

    ret_numsub = []
    for channel, numsub in numsub_d.items():
        ret_numsub.append((channel, numsub))
    return ret_numsub
