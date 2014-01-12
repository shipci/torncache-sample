# -*- mode: python; coding: utf-8 -*-

"""
Protocol. A Base class to implement cached protocols
"""

from __future__ import absolute_import

import os
import re
import socket
import weakref
import itertools

# For MC url parsing
try:
    import urlparse  # py2
except ImportError:
    basestring = str
    import urllib.parse as urlparse  # py3

# tornado requirements
from tornado.ioloop import IOLoop

# local requirements
from torncache.distributions import Distribution

class ProtocolError(Exception):
    """Resource exception"""


class Protocols(object):
    """Utility methods for instantiate valid protocol class"""

    _PROTOCOLS = {}

    @classmethod
    def register(cls, scheme, protocol):
        """Register a converter for the given type"""
        cls._PROTOCOLS[scheme] = protocol

    @classmethod
    def fetch(cls, data):
        """Return a converter for the given type"""
        # try to fetch directly from a_type string
        if data:
            data = [data] if isinstance(data, basestring) else data
            chunk_filter = lambda x: isinstance(x, basestring)
            for chunk in itertools.ifilter(chunk_filter, data):
                proto = cls._PROTOCOLS.get(chunk)
                if proto is None:
                    proto = cls._PROTOCOLS.get(urlparse.urlparse(chunk).scheme)
                if proto:
                    return proto
        return None


class MetaProtocol(type):
    """Protocol Metaclass"""

    def __init__(cls, name, bases, dct):
        type.__init__(cls, name, bases, dct)
        if name.endswith("Protocol"):
            for scheme in cls.SCHEMES:
                Protocols.register(scheme, cls)


class ProtocolMixin(object):
    """
    Abstract class to define converter classes. This class has support
    to create chained converters
    """

    __metaclass__ = MetaProtocol

    CLIENTS = weakref.WeakKeyDictionary()
    SCHEMES = []
    DEFAULT_PORT = None
    CONNECTION = None

    def __init__(self, servers, ioloop=None,
                 serializer=None, deserializer=None,
                 connect_timeout=5, timeout=1, no_delay=True,
                 ignore_exc=True, dead_retry=30,
                 hash_tags=None, distribution=None,
                 server_retries=10):

        # Watcher to destroy client when ioloop expires
        self._ioloop = ioloop or IOLoop.instance()
        self.CLIENTS[self._ioloop] = self

        # srv args
        self._conn = self.CONNECTION
        self._conn_options = {
            'ioloop': self._ioloop,
            'serializer': serializer,
            'deserializer': deserializer,
            'connect_timeout': connect_timeout,
            'timeout': timeout,
            'no_delay': no_delay,
            'ignore_exc': ignore_exc,
            'dead_retry': dead_retry
        }
        # srvs
        self._servers = {}
        self._server_retries = server_retries
        # create dist and init servers
        self.dist = Distribution.create(distribution, hash_tags=hash_tags)
        self.connect(servers)

    def __del__(self):
        # Free connections
        self.close()

    def connect(self, servers):
        # Connect to servers
        nodes = {}
        for name, server in servers:
            server = self._conn(server, **self._conn_options)
            self._servers[name] = server
            nodes[name] = server.weight
        self.dist.add_nodes(nodes)

    def close(self):
        # Free resources
        [server.close() for server in self._servers.itervalues()]
        self._servers = {}
        self.dist.clear()

    def _find_server(self, value):
        """Find a server from a string"""
        if isinstance(value, self.CONNECTION):
            return value
        # check if match server name or address
        for name, candidate in self._servers.iteritems():
            if str(candidate) == value or name == value:
                return candidate
        # try with a key
        return self._get_server(value)[0]

    def _get_server(self, key):
        # get pair server, key
        for i in xrange(self._server_retries):
            for name in self.dist.iterate_nodes(key):
                if name is None:
                    # No servers alive
                    return None, None
                server = self._servers[name]
                if server.is_alive():
                    return server, key
        return None, None

    def _shard(self, keys):
        keys = ",".split(keys) if isinstance(keys, basestring) else keys
        retval = {}
        for key in keys:
            server, _ = self._get_server(key)
            retval.setdefault(server, []).append(key)
        return retval


    @classmethod
    def parse_servers(cls, servers):
        _servers = []

        # Parse servers if it's a collection of urls
        servers = servers or []
        if isinstance(servers, basestring):
            servers = servers.split(',')

        for server in servers:
            # parse url form 'mc://host:port?<weight>='
            if any(itertools.imap(server.startswith, cls.SCHEMES)):
                url = urlparse.urlsplit(server)
                server = url.netloc or url.path
                params = urlparse.parse_qs(url.query)
                server = [params.get('name'), (server, params.get('weight'))]
            # twemproxy like string
            else:
                rx = '(?P<host>\w+(:\d+)?)(:(?P<weight>\d+))?( (?P<name>.*))?'
                matchs = re.match(rx, server).groups
                server = (matchs('name'), (matchs('host'), matchs('weight')))
            # Append
            _servers.append(server)

        # add port to tuples if missing
        retval = {}
        for name, host in _servers:
            weight, port = 1, cls.DEFAULT_PORT
            # extract host and weight from tuple
            if not isinstance(host, basestring):
                if len(host) > 1:
                    weight = host[1] or 1
                host = host[0]
            # extract host and port
            if ':' in host:
                host, _, port = host.partition(':')
            # unix socket ?
            if os.path.exists(host):
                retval[name or host] = (host, weight)
            else:
                # resolve host
                candidates = socket.getaddrinfo(
                    host, port,
                    socket.AF_INET, socket.SOCK_STREAM)
                for candidate in candidates:
                    host = "{0}:{1}".format(*candidate[4])
                    _, weight = retval.get(name or host, (None, weight - 1))
                    retval[name or host] = (host, weight + 1)
        # Return well formatted list of servers
        return retval.items()


# Register protocols
import torncache.protocols
