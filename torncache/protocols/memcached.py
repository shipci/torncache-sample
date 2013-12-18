# -*- mode: python; coding: utf-8 -*-

"""
Protocol. A Base class to implement cached protocols
"""

from __future__ import absolute_import

import itertools
import functools

from tornado import stack_context
from tornado.gen import engine, Task

# Local requirements
from torncache.connection import Connection
from torncache.protocol import ProtocolMixin

VALID_STORE_RESULTS = {
    'set':     ('STORED',),
    'add':     ('STORED', 'NOT_STORED'),
    'replace': ('STORED', 'NOT_STORED'),
    'append':  ('STORED', 'NOT_STORED'),
    'prepend': ('STORED', 'NOT_STORED'),
    'cas':     ('STORED', 'EXISTS', 'NOT_FOUND'),
}


# Some of the values returned by the "stats" command
# need mapping into native Python types
STAT_TYPES = {
    # General stats
    'version': str,
    'rusage_user': lambda value: float(value.replace(':', '.')),
    'rusage_system': lambda value: float(value.replace(':', '.')),
    'hash_is_expanding': lambda value: int(value) != 0,
    'slab_reassign_running': lambda value: int(value) != 0,

    # Settings stats
    'inter': str,
    'evictions': lambda value: value == 'on',
    'growth_factor': float,
    'stat_key_prefix': str,
    'umask': lambda value: int(value, 8),
    'detail_enabled': lambda value: int(value) != 0,
    'cas_enabled': lambda value: int(value) != 0,
    'auth_enabled_sasl': lambda value: value == 'yes',
    'maxconns_fast': lambda value: int(value) != 0,
    'slab_reassign': lambda value: int(value) != 0,
    'slab_automove': lambda value: int(value) != 0,
}


class MemcacheError(Exception):
    "Base exception class"


class MemcacheClientError(MemcacheError):
    """Raised when memcached fails to parse the arguments to a request, likely
    due to a malformed key and/or value, a bug in this library, or a version
    mismatch with memcached."""


class MemcacheUnknownCommandError(MemcacheClientError):
    """Raised when memcached fails to parse a request, likely due to a bug in
    this library or a version mismatch with memcached."""


class MemcacheIllegalInputError(MemcacheClientError):
    """Raised when a key or value is not legal for Memcache (see the class docs
    for Client for more details)."""


class MemcacheServerError(MemcacheError):
    """Raised when memcached reports a failure while processing a request,
    likely due to a bug or transient issue in memcached."""


class MemcacheUnknownError(MemcacheError):
    """Raised when this library receives a response from memcached that it
    cannot parse, likely due to a bug in this library or a version mismatch
    with memcached."""


class MemcacheUnexpectedCloseError(MemcacheServerError):
    "Raised when the connection with memcached closes unexpectedly."


class MemcachedConnection(Connection):
    """Custom connection class to connect to Memcached servers"""


class MemcachedProtocol(ProtocolMixin):

    SCHEMES = ['mc', 'memcached']
    DEFAULT_PORT = 11211
    CONNECTION = MemcachedConnection

    def __init__(self, *args, **kwargs):
        super(MemcachedProtocol, self).__init__(*args, **kwargs)
        self.parser = MemcachedParser

    def set(self, key, value, expire=0, noreply=True, callback=None):
        """
        The memcached "set" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          If no exception is raised, always returns True. If an exception is
          raised, the set may or may not have occurred. If noreply is True,
          then a successful return does not guarantee a successful set.
        """
        # Fetch memcached connection
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return
        # invoke
        self.parser.store_cmd(
            server, 'set', key, expire, noreply, value, None, callback)

    def set_many(self, values, expire=0, noreply=True, callback=None):
        """A convenience function for setting multiple values.

        Args:
          values: dict(str, str), a dict of keys and values, see class docs
                  for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          Returns a dictionary of keys and operations result
          values. For each entry, if no exception is raised, always
          returns True. If an exception is raised, the set may or may
          not have occurred. If noreply is True, then a successful
          return does not guarantee a successful set. If no server is
          present, None is returned.
        """
        # response handler
        def on_response(key, result):
            retval[key] = result
            if len(retval) == len(values):
                callback and callback(retval)

        # shortcut
        if not values:
            callback and callback({})

        # init vars
        retval, servers = dict(), dict()
        for key, value in values.iteritems():
            server, key = self._get_server(key)
            servers[key] = server
        # set it
        for key, server in servers.iteritems():
            if server is None:
                on_response(key, False)
                continue
            cb = stack_context.wrap(functools.partial(on_response, key))
            self.set(key, value, expire, noreply, callback=cb)

    def add(self, key, value, expire=0, noreply=True, callback=None):
        """
        The memcached "add" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          If noreply is True, the return value is always True. Otherwise the
          return value is True if the value was stgored, and False if it was
          not (because the key already existed).
        """
        # Fetch memcached connection
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return
        # invoke
        self.parser.store_cmd(
            server, 'add', key, expire, noreply, value, None, callback)

    def replace(self, key, value, expire=0, noreply=True, callback=None):
        """
        The memcached "replace" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          If noreply is True, always returns True. Otherwise returns True if
          the value was stored and False if it wasn't (because the key didn't
          already exist).
        """
        # Fetch memcached connection
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return
        # invoke
        self.parser.store_cmd(
            server, 'replace', key, expire, noreply, value, None, callback)

    def append(self, key, value, expire=0, noreply=True, callback=None):
        """
        The memcached "append" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          True.
        """
        # Fetch memcached connection
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return
        # invoke
        self.parser.store_cmd(
            server, 'append', key, expire, noreply, value, None, callback)

    def prepend(self, key, value, expire=0, noreply=True, callback=None):
        """
        The memcached "prepend" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          True.
        """
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return
        # invoke
        self.parser.store_cmd(
            server, 'prepend', key, expire, noreply, value, None, callback)

    def cas(self, key, value, cas, expire=0, noreply=False, callback=None):
        """
        The memcached "cas" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          cas: int or str that only contains the characters '0'-'9'.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, False to wait for the reply (the default).

        Returns:
          If noreply is True, always returns True. Otherwise returns None if
          the key didn't exist, False if it existed but had a different cas
          value and True if it existed and was changed.
        """
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return
        # invoke
        self.parser.store_cmd(
            server, 'cas', key, expire, noreply, value, cas, callback)

    def get(self, key, callback):
        """
        The memcached "get" command, but only for one key, as a convenience.

        Args:
          key: str, see class docs for details.

        Returns:
          The value for the key, or None if the key wasn't found.
        """
        server, key = self._get_server(key)
        if not server:
            callback(None)
            return

        cb = lambda x: callback(x.get(key, None))
        self.parser.fetch_cmd(server, 'get', [key], False, callback=cb)

    def get_many(self, keys, callback):
        """
        The memcached "get" command.

        Args:
          keys: list(str), see class docs for details.

        Returns:
          A dict in which the keys are elements of the "keys" argument list
          and the values are values from the cache. The dict may contain all,
          some or none of the given keys.
        """
        # response handler
        def on_response(server, result):
            retval.update(result)
            pending.remove(server)
            if len(pending) == 0:
                callback(retval)

        # shortcut
        if not keys:
            callback({})

        # init vars
        retval, servers = dict(), dict()
        for key in keys:
            server, key = self._get_server(key)
            servers.setdefault(server, [])
            servers[server].append(key)
        # set it
        pending = servers.keys()
        for server, keys in servers.iteritems():
            if server is None:
                result = itertools.izip_longest(keys, [], fillvalue=None)
                on_response(server, result)
                continue
            cb = stack_context.wrap(functools.partial(on_response, server))
            self.parser.fetch_cmd(server, 'get', keys, False, callback=cb)

    def gets(self, key, callback):
        """
        The memcached "gets" command for one key, as a convenience.

        Args:
          key: str, see class docs for details.

        Returns:
          A tuple of (key, cas), or (None, None) if the key was not found.
        """
        server, key = self._get_server(key)
        if not server:
            callback((None, None))
            return

        cb = lambda x: callback(x.get(key, (None, None)))
        self.parser.fetch_cmd(server, 'gets', [key], True, callback=cb)

    def gets_many(self, keys, callback):
        """
        The memcached "gets" command.

        Args:
          keys: list(str), see class docs for details.

        Returns:
          A dict in which the keys are elements of the "keys" argument list and
          the values are tuples of (value, cas) from the cache. The dict may
          contain all, some or none of the given keys.
        """
        # response handler
        def on_response(server, result):
            retval.update(result)
            pending.remove(server)
            if len(pending) == 0:
                callback(retval)

        # shortcut
        if not keys:
            callback({})

        # init vars
        responses, retval, servers = [], dict(), dict()
        for key in keys:
            server, key = self._get_server(key)
            servers.setdefault(server, [])
            servers[server].append(key)
        # set it
        pending = servers.keys()
        for server, keys in servers.iteritems():
            if server is None:
                result = itertools.izip_longest(keys, [], fillvalue=None)
                on_response(server, result)
                continue
            cb = stack_context.wrap(functools.partial(on_response, server))
            self.parser.fetch_cmd(server, 'gets', keys, True, callback=cb)

    def delete(self, key, time=0, noreply=True, callback=None):
        """
        The memcached "delete" command.

        Args:
          key: str, see class docs for details.

        Returns:
          If noreply is True, always returns True. Otherwise returns True if
          the key was deleted, and False if it wasn't found.
        """
        def on_response(data):
            callback(data.startswith('DELETED'))

        # Fetch memcached connection
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return
        # compute command
        timearg = ' {0}'.format(time) if time else ''
        replarg = ' noreply' if noreply else ''
        cmd = 'delete {0}{1}{2}\r\n'.format(key, timearg, replarg)

        # invoke
        cb = callback if noreply else stack_context.wrap(on_response)
        self.parser.misc_cmd(server, cmd, 'delete', noreply, callback=cb)

    def delete_many(self, keys, noreply=True, callback=None):
        """
        A convenience function to delete multiple keys.

        Args:
          keys: list(str), the list of keys to delete.

        Returns:
          True. If an exception is raised then all, some or none of the keys
          may have been deleted. Otherwise all the keys have been sent to
          memcache for deletion and if noreply is False, they have been
          acknowledged by memcache.
        """
        # response handler
        def on_response(key, result):
            retval[key] = result
            if len(retval) == len(keys):
                callback and callback(retval)

        if not keys:
            callback and callback({})

        # init vars
        retval, servers = dict(), dict()
        for key in keys:
            server, key = self._get_server(key)
            servers[key] = server
        # set it
        for key, server in servers.iteritems():
            if server is None:
                on_response(key, False)
                continue
            cb = stack_context.wrap(functools.partial(on_response, key))
            self.delete(key, noreply, callback=cb)

    def incr(self, key, value, noreply=False, callback=None):
        """
        The memcached "incr" command.

        Args:
          key: str, see class docs for details.
          value: int, the amount by which to increment the value.
          noreply: optional bool, False to wait for the reply (the default).

        Returns:
          If noreply is True, always returns None. Otherwise returns the new
          value of the key, or False if the key wasn't found.
        """
        def on_response(data):
            result = False if data.startswith('NOT_FOUND') else int(data)
            callback and callback(result)

        # Fetch memcached connection
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return

        replarg = ' noreply' if noreply else ''
        cmd = "incr {0} {1}{2}\r\n".format(key, str(value), replarg)

        # invoke
        cb = callback if noreply else stack_context.wrap(on_response)
        self.parser.misc_cmd(server, cmd, 'incr', noreply, callback=cb)

    def decr(self, key, value, noreply=False, callback=None):
        """
        The memcached "decr" command.

        Args:
          key: str, see class docs for details.
          value: int, the amount by which to increment the value.
          noreply: optional bool, False to wait for the reply (the default).

        Returns:
          If noreply is True, always returns None. Otherwise returns the new
          value of the key, or False if the key wasn't found.
        """
        def on_response(data):
            result = False if data.startswith('NOT_FOUND') else int(data)
            callback and callback(result)

        # Fetch memcached connection
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return

        replarg = ' noreply' if noreply else ''
        cmd = "decr {0} {1}{2}\r\n".format(key, str(value), replarg)

        # invoke
        cb = callback if noreply else stack_context.wrap(on_response)
        self.parser.misc_cmd(server, cmd, 'decr', noreply, callback=cb)

    def touch(self, key, expire=0, noreply=True, callback=None):
        """
        The memcached "touch" command.

        Args:
          key: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          True if the expiration time was updated, False if the key wasn't
          found.
        """
        def on_response(data):
            callback and callback(data.startswith('TOUCHED'))

        # Fetch memcached connection
        server, key = self._get_server(key)
        if not server:
            callback and callback(None)
            return

        replarg = ' noreply' if noreply else ''
        cmd = "touch {0} {1}{2}\r\n".format(key, expire, replarg)

        # invoke
        cb = callback if noreply else stack_context.wrap(on_response)
        self.parser.misc_cmd(server, cmd, 'touch', noreply, callback=cb)

    def stats(self, server, *args, **kwargs):
        """
        The memcached "stats" command.

        The returned keys depend on what the "stats" command returns.
        A best effort is made to convert values to appropriate Python
        types, defaulting to strings when a conversion cannot be made.

        Args:
          *arg: extra string arguments to the "stats" command. See the
                memcached protocol documentation for more information.

        Returns:
          A dict of the returned stats.
        """
        def on_response(data):
            result = {}
            for key, value in data.iteritems():
                converter = STAT_TYPES.get(key, int)
                try:
                    result[key] = converter(value)
                except Exception:
                    pass
            callback(result)

        # Fetch memcached connection
        callback, server = kwargs['callback'], self._find_server(server)
        if not server:
            callback(None)
            return

        # invoke
        cb = stack_context.wrap(on_response)
        self.parser.fetch_cmd(server, 'stats', args, False, callback=cb)

    def flush_all(self, server, delay=0, noreply=True, callback=None):
        """
        The memcached "flush_all" command.

        Args:
          delay: optional int, the number of seconds to wait before flushing,
                 or zero to flush immediately (the default).
          noreply: optional bool, True to not wait for the response
                 (the default).

        Returns:
          True.
        """
        def on_response(data):
            callback and callback(data.startswith('OK'))

        # Fetch memcached connection
        server = self._find_server(server)
        if not server:
            callback and callback(None)
            return

        replarg = ' noreply' if noreply else ''
        cmd = "flush_all {0} {1}\r\n".format(delay, replarg)

        # invoke
        cb = callback if noreply else stack_context.wrap(on_response)
        self.parser.misc_cmd(server, cmd, 'flush_all', noreply, callback=cb)

    def quit(self, server, callback=None):
        """
        The memcached "quit" command.

        This will close the connection with memcached. Calling any other
        method on this object will re-open the connection, so this object can
        be re-used after quit.
        """
        def on_response(result):
            server.close()
            callback and callback(result)

        # Fetch memcached connection
        server = self._find_server(server)
        if not server:
            raise MemcacheClientError("Unknown Server {0}".format(server))

        cmd = "quit\r\n"
        cb = stack_context.wrap(on_response)
        self.parser.misc_cmd(server, cmd, 'quit', True, callback=cb)


class MemcachedParser(object):

    @staticmethod
    def _raise_errors(line, name):
        if line.startswith('ERROR'):
            raise MemcacheUnknownCommandError(name)

        if line.startswith('CLIENT_ERROR'):
            error = line[line.find(' ') + 1:]
            raise MemcacheClientError(error)

        if line.startswith('SERVER_ERROR'):
            error = line[line.find(' ') + 1:]
            raise MemcacheServerError(error)

    @staticmethod
    @engine
    def fetch_cmd(conn, name, keys, expect_cas, callback):
        # build command
        try:
            key_strs = []
            for key in keys:
                key = str(key)
                if ' ' in key:
                    error = "Key contains spaces: {0}".format(key)
                    raise MemcacheIllegalInputError(error)
                key_strs.append(key)
        except UnicodeEncodeError as e:
            raise MemcacheIllegalInputError(str(e))

        try:
            # Open connection if required
            conn.closed() and (yield Task(conn.connect))

            # Add timeout for this request
            conn._add_timeout("Timeout on fetch '{0}'".format(name))

            result = {}
            # send command

            cmd = '{0} {1}\r\n'.format(name, ' '.join(key_strs))
            _ = yield Task(conn._stream.write, cmd)
            # parse response
            while True:
                line = yield Task(conn.read)
                MemcachedParser._raise_errors(line, name)

                if line == 'END':
                    break
                elif line.startswith('VALUE'):
                    if expect_cas:
                        _, key, flags, size, cas = line.split()
                    else:
                        _, key, flags, size = line.split()
                    # read also \r\n
                    value = yield Task(conn.read, int(size))
                    if conn._deserializer:
                        value = conn._deserializer(key, value, int(flags))
                    if expect_cas:
                        result[key] = (value, cas)
                    else:
                        result[key] = value
                elif name == 'stats' and line.startswith('STAT'):
                    _, key, value = line.split()
                    result[key] = value
                else:
                    raise MemcacheUnknownError(line[:32])

        except Exception as err:
            if isinstance(err, (IOError, OSError)):
                conn.mark_dead(str(err))
            if conn._ignore_exc:
                conn._clear_timeout()
                callback({})
                return
            raise
        #return result
        conn._clear_timeout()
        callback(result)

    @staticmethod
    @engine
    def store_cmd(conn, name, key, expire, noreply, data,
                  cas=None, callback=None):
        try:
            # process key
            key = str(key)
            if ' ' in key:
                raise MemcacheIllegalInputError("Key contains spaces: %s", key)
            # process cas. Only digits are allowed by memcached
            # if cas is not None:
            #     cas = str(cas)
            #     if not cas.isdigit():
            #         MemcacheIllegalInputError("Digit based cas was expected")
            # process data
            flags = 0
            if conn._serializer:
                data, flags = conn._serializer(key, data)
            data = str(data)
        except UnicodeEncodeError as e:
            raise MemcacheIllegalInputError(str(e))

        # compute cmd
        if cas is not None and noreply:
            extra = ' {0} noreply'.format(cas)
        elif cas is not None and not noreply:
            extra = ' {0}'.format(cas)
        elif cas is None and noreply:
            extra = ' noreply'
        else:
            extra = ''

        cmd = '{0} {1} {2} {3} {4}{5}\r\n{6}\r\n'.format(
            name, key, flags, expire, len(data), extra, data)

        try:
            # Open connection if required
            conn.closed() and (yield Task(conn.connect))

            # Add timeout for this request
            conn._add_timeout("Timeout on fetch '{0}'".format(name))

            yield Task(conn.write, cmd)
            if noreply:
                conn._clear_timeout()
                callback and callback(True)
                return

            line = yield Task(conn.read)
            MemcachedParser._raise_errors(line, name)
            conn._clear_timeout()

            if line in VALID_STORE_RESULTS[name]:
                if line == 'STORED':
                    callback(True)
                elif line == 'NOT_STORED':
                    callback(False)
                # only for cas related actions
                elif line == 'NOT_FOUND':
                    callback(None)
                elif line == 'EXISTS':
                    callback(False)
            else:
                raise MemcacheUnknownError(line[:32])
        except Exception as err:
            if isinstance(err, (IOError, OSError)):
                conn.mark_dead(str(err))
            if conn._ignore_exc:
                conn._clear_timeout()
                callback and callback(None)
                return
            raise

    @staticmethod
    @engine
    def misc_cmd(conn, cmd, cmd_name, noreply, callback=None):

        try:
            # Open connection if required
            conn.closed() and (yield Task(conn.connect))

            # Add timeout for this request
            conn._add_timeout("Timeout on misc '{0}'".format(cmd_name))

            # send command
            yield Task(conn.write, cmd)
            if noreply:
                conn._clear_timeout()
                callback and callback(True)
                return

            # wait for response
            line = yield Task(conn.read)
            MemcachedParser._raise_errors(line, cmd_name)

        except Exception as err:
            if isinstance(err, (IOError, OSError)):
                conn.mark_dead(str(err))
            if conn._ignore_exc:
                conn._clear_timeout()
                callback and callback(None)
                return
            raise
        # return result
        conn._clear_timeout()
        callback and callback(line)
