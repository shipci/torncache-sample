# -*- mode: python; coding: utf-8 -*-

"""
Torncache Pool
"""

from __future__ import absolute_import

import weakref
import functools
import collections


# local requirements
from torncache.protocol import Protocols


class ClientPoolError(Exception):
    """Raised when number of clients excees size"""


class ClientPool(object):
    """A Pool of clients"""

    class _BroadCast(object):
        """
        A Private decorator to broadcast some calls like flush_all to all
        servers
        """
        def __init__(self, pool):
            self.pool = pool

        def __getattr__(self, name):
            if hasattr(self.pool._proto, name):
                return functools.partial(self._invoke, name)
            # raise AttributeError
            raise AttributeError(name)

        def _invoke(self, cmd, *args, **kwargs):
            def on_finish(response, host, _cb):
                retval[host] = response
                if len(retval) == len(self.pool._servers):
                    _cb and _cb(retval)
            # invoke and collect results
            retval = {}
            cb = kwargs.get('callback')
            for host, _ in self.pool._servers:
                kwargs['callback'] = functools.partial(on_finish, host, _cb=cb)
                func = functools.partial(getattr(self.pool, cmd), host)
                func(*args, **kwargs)

    def __init__(self, servers, size=0, **kwargs):
        # Try to fetch protocol from parameters or infere from server details
        self._proto = Protocols.fetch(kwargs.get('protocol', servers))
        if self._proto is None:
            raise ClientPoolError("Unkown protocol or protocol not specified")
        # parse server strings
        self._servers = self._proto.parse_servers(servers)
        self._size = size
        self._used = weakref.WeakSet()
        self._clients = collections.deque()
        # Client arguments
        self._kwargs = kwargs

    def _create_clients(self, n):
        return [self._proto(self._servers, **self._kwargs) for x in xrange(n)]

    def _get_client(self, private=False, shared=False):
        if private:
            return self._create_clients(1)[0]
        if not self._clients:
            # Add a new client
            total_clients = len(self._clients) + len(self._used)
            if self._size > 0 and total_clients >= self._size:
                error = "Max of %d clients is already reached" % self._size
                raise ClientPoolError(error)
            self._clients.append(*self._create_clients(1))
        # fetch available one
        if shared:
            # If shared, it's expected that we are not going to use
            # connection functionality from client, for example, for
            # key tag stuff
            client = self._clients[0]
        else:
            client = self._clients.popleft()
            self._used.add(client)
        return client

    def _free_client(self, client):
        self._used.remove(client)
        self._clients.append(client)

    def _invoke(self, cmd, *args, **kwargs):
        def on_finish(response, c, _cb, **kwargs):
            self._free_client(c)
            _cb and _cb(response, **kwargs)

        if not self._clients:
            # Add a new client
            total_clients = len(self._clients) + len(self._used)
            if self._size > 0 and total_clients >= self._size:
                error = "Max of %d clients is already reached" % self._size
                raise ClientPoolError(error)
            self._clients.append(*self._create_clients(1))
        # fetch available one
        client = self._get_client()
        # override used callback to
        cb = kwargs.get('callback')
        kwargs['callback'] = functools.partial(on_finish, c=client, _cb=cb)
        getattr(client, cmd)(*args, **kwargs)

    def __getattr__(self, name):
        if name in ('shard', 'register_script'):
            return getattr(self._get_client(private=True), name)
        if name in ('tag_key', 'untag_key'):
            return getattr(self._get_client(shared=True).dist, name)
        if name == 'pipeline':
            return self._get_client().pipeline
        if name == 'broadcast':
            return self._BroadCast(self)
        # common case
        if hasattr(self._proto, name):
            return functools.partial(self._invoke, name)
        # raise error
        raise AttributeError(name)

    @property
    def protocol(self):
        """Get protocol used for this pool"""
        return self._proto.SCHEMES
