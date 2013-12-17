# -*- mode: python; coding: utf-8 -*-

"""
Protocol. A Base class to implement cached protocols
"""

from __future__ import absolute_import

import os
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
from torncache.connection import Connection


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

    def __init__(self, servers, ioloop=None,
                 serializer=None, deserializer=None,
                 connect_timeout=5, timeout=1, no_delay=True,
                 ignore_exc=True, dead_retry=30,
                 server_retries=10):

        # Watcher to destroy client when ioloop expires
        self._ioloop = ioloop or IOLoop.instance()
        self.CLIENTS[self._ioloop] = self

        self._server_retries = server_retries
        self._server_args = {
            'ioloop': self._ioloop,
            'serializer': serializer,
            'deserializer': deserializer,
            'connect_timeout': connect_timeout,
            'timeout': timeout,
            'no_delay': no_delay,
            'ignore_exc': ignore_exc,
            'dead_retry': dead_retry
        }

        # servers
        self._servers = []
        self._buckets = []

        # Servers can be passed in three forms:
        #    1. Strings of the form C{"host:port"}, which implies a
        #    default weight of 1.
        #    2. Tuples of the form C{("host:port", weight)}, where C{weight} is
        #    an integer weight value.
        #    3. Tuples of the form C{"path", weight} where path points to
        #    a local unix path
        for server in servers:
            server = Connection(server, **self._server_args)
            for i in xrange(server.weight):
                self._buckets.append(server)
            self._servers.append(server)

    def _find_server(self, value):
        """Find a server from a string"""
        if isinstance(value, Connection):
            return value
        # check if server is an address
        for candidate in self._servers:
            if str(candidate) == value:
                return candidate
        # try with a key
        return self._get_server(value)[0]

    def _get_server(self, key):
        """Fetch valid MC for this key"""
        serverhash = 0
        if isinstance(key, tuple):
            serverhash, key = key[:2]
        elif len(self._buckets) > 1:
            serverhash = hash(key)
        # get pair server, key
        return (self._buckets[serverhash % len(self._buckets)], key)

    @classmethod
    def parse_servers(cls, servers):
        _servers = servers or []
        # Parse servers if it's a collection of urls
        if isinstance(servers, basestring):
            _servers = []
            for server in servers.split(','):
                # parse url form 'mc://host:port?<weight>='
                if any(itertools.imap(server.startswith, cls.SCHEMES)):
                    url = urlparse.urlsplit(server)
                    server = url.netloc or url.path
                    if url.query:
                        weight = urlparse.parse_qs(url.query).get('weight', 1)
                        server = [server, weight]
                _servers.append(server)
        # add port to tuples if missing
        retval = {}
        for host in _servers:
            weight, port = 1, cls.DEFAULT_PORT
            # extract host and weight from tuple
            if not isinstance(host, basestring):
                if len(host) > 1:
                    weight = host[1]
                host = host[0]
            # extract host and port
            if ':' in host:
                host, _, port = host.partition(':')
            # unix socket ?
            if os.path.exists(host):
                retval[host] = weight
            else:
                # resolve host
                candidates = socket.getaddrinfo(
                    host, port,
                    socket.AF_INET, socket.SOCK_STREAM)
                for candidate in candidates:
                    host = "{0}:{1}".format(*candidate[4])
                    weight = retval.get(host, weight - 1)
                    retval[host] = weight + 1
        # Return well formatted list of servers
        return retval.items()


# Register protocols
import torncache.protocols
