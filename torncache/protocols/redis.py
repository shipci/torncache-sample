# -*- mode: python; coding: utf-8 -*-

"""
Protocol. A Base class to implement cached protocols
"""

from __future__ import absolute_import

import datetime
import itertools
import time as mod_time

# redis deps
import redis
import hiredis

# Local requirements
from torncache.protocol import ProtocolMixin
from torncache.connection import Connection

# tornado deps
from tornado.gen import engine, Task

b = lambda x: x

SYM_STAR = b('*')
SYM_DOLLAR = b('$')
SYM_CRLF = b('\r\n')
SYM_LF = b('\n')
SYM_EMPTY = b('')


class RedisConnection(Connection):
    """Custom connection to redis servers"""


class RedisParser(object):
    """A Redis protocol parser"""
    def __init__(self, **options):
        self.encoding = options.get('encoding', 'utf-8')
        self.encoding_errors = options.get('encoding_errors', 'strict')
        self.decode_responses = options.get('decode_responses', False)
        # init responses parser
        hoptions = {}
        if self.decode_responses:
            hoptions['encoding'] = self.encoding
        self.reader = hiredis.Reader(**hoptions)
        # init response callbacks
        self.response_callbacks = options.get('response_callbacks')
        # If not response callbacks available
        if not self.response_callbacks:
            callbacks = redis.client.StrictRedis.RESPONSE_CALLBACKS
            self.response_callbacks = callbacks

    def encode(self, value):
        "Return a bytestring representation of the value"
        if isinstance(value, bytes):
            return value
        if isinstance(value, float):
            value = repr(value)
        if not isinstance(value, basestring):
            value = str(value)
        if isinstance(value, unicode):
            value = value.encode(self.encoding, self.encoding_errors)
        return value

    def pack_command(self, *args):
        "Pack a series of arguments into a value Redis command"
        args_output = SYM_EMPTY.join([
            SYM_EMPTY.join((SYM_DOLLAR, b(str(len(k))), SYM_CRLF, k, SYM_CRLF))
            for k in itertools.imap(self.encode, args)])
        output = SYM_EMPTY.join(
            (SYM_STAR, b(str(len(args))), SYM_CRLF, args_output))
        return output

    def send_command(self, conn, command, callback=None):
        "Send an already packed command to the Redis server"
        conn.write(command, callback)

    @engine
    def parse_response(self, conn, command, options, callback):
        "Parse response to a command"
        response = False
        while(response is False):
            data = yield Task(conn.readline)
            self.reader.feed(data)
            response = self.reader.gets()
        if command in self.response_callbacks:
            response = self.response_callbacks[command](response, **options)
        callback(response)


class RedisProtocol(ProtocolMixin):

    SCHEMES = ['rd', 'redis']
    DEFAULT_PORT = 6379
    CONNECTION = RedisConnection

    def __init__(self, *args, **kwargs):
        super(RedisProtocol, self).__init__(*args, **kwargs)
        self.parser = RedisParser(**kwargs)

    def _get_server(self, *args, **options):
        connection, cmd, args = None, None, args
        if isinstance(args[0], basestring):
            connection, cmd = super(RedisProtocol, self)._get_server(args[0])
        else:
            connection = args.pop(0)
            cmd = args[0]
        # return tupple
        return (connection, cmd, args)

    @engine
    def execute_command(self, *args, **options):
        "Execute a command and return a parsed response"

        # first arg could be a command or a connection
        conn, cmd, args = self._get_server(*args, **options)
        # get callback
        if 'callback' in options:
            callback = options.pop('callback')
        # get timeout
        timeout = None
        if 'timeout' in options:
            timeout = options.pop('timeout')

        try:
            # Open connection if required
            conn.closed() and (yield Task(conn.connect))
            # Add timeout for this request
            conn._add_timeout("Timeout on command '{0}'".format(cmd), timeout)
            # parse command
            cmd = self.parser.pack_command(*args)
            # send command
            yield Task(self.parser.send_command, conn, cmd)
            # read response
            retval = yield Task(self.parser.parse_response, conn, cmd, options)

        except Exception as err:
            raise err
            if isinstance(err, (IOError, OSError)):
                conn.mark_dead(str(err))
            if conn._ignore_exc:
                conn._clear_timeout()
                callback and callback(None)
                return
            raise
        #return result
        conn._clear_timeout()
        callback and callback(retval)

    #### SERVER INFORMATION ####
    def bgrewriteaof(self, callback=None):
        "Tell the Redis server to rewrite the AOF file from data in memory."
        self.execute_command('BGREWRITEAOF', callback=callback)

    def bgsave(self, callback=None):
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """
        self.execute_command('BGSAVE', callback=callback)

    def client_list(self, callback=None):
        "Returns a list of currently connected clients"
        self.execute_command('CLIENT', 'LIST', parse='LIST', callback=callback)

    def client_getname(self, callback=None):
        "Returns the current connection name"
        self.execute_command(
            'CLIENT', 'GETNAME',
            parse='GETNAME', callback=callback)

    def client_setname(self, name, callback=None):
        "Sets the current connection name"
        self.execute_command(
            'CLIENT', 'SETNAME',
            name, parse='SETNAME', callback=callback)

    def config_get(self, pattern="*", callback=None):
        "Return a dictionary of configuration based on the ``pattern``"
        self.execute_command(
            'CONFIG', 'GET',
            pattern, parse='GET', callback=callback)

    def config_set(self, name, value, callback=None):
        "Set config item ``name`` with ``value``"
        self.execute_command(
            'CONFIG', 'SET',
            name, value, parse='SET', callback=callback)

    def config_resetstat(self, callback=None):
        "Reset runtime statistics"
        self.execute_command(
            'CONFIG', 'RESETSTAT',
            parse='RESETSTAT', callback=callback)

    def dbsize(self, callback=None):
        "Returns the number of keys in the current database"
        self.execute_command('DBSIZE', callback=callback)

    def debug_object(self, key, callback=None):
        "Returns version specific metainformation about a give key"
        self.execute_command('DEBUG', 'OBJECT', key, callback=callback)

    def echo(self, value, callback=None):
        "Echo the string back from the server"
        self.execute_command('ECHO', value, callback=callback)

    def flushall(self, callback=None):
        "Delete all keys in all databases on the current host"
        self.execute_command('FLUSHALL', callback=callback)

    def flushdb(self, callback=None):
        "Delete all keys in the current database"
        self.execute_command('FLUSHDB', callback=callback)

    def info(self, section=None, callback=None):
        """
        Returns a dictionary containing information about the Redis server

        The ``section`` option can be used to select a specific section
        of information

        The section option is not supported by older versions of Redis Server,
        and will generate ResponseError
        """
        args = ('INFO', )
        if section:
            args += (section, )
        self.execute_command(*args, callback=callback)

    def lastsave(self, callback=None):
        """
        Return a Python datetime object representing the last time the
        Redis database was saved to disk
        """
        self.execute_command('LASTSAVE', callback=callback)

    def object(self, infotype, key, callback=None):
        "Return the encoding, idletime, or refcount about the key"
        self.execute_command(
            'OBJECT',
            infotype, key, infotype=infotype, callback=callback)

    def ping(self, callback=None):
        "Ping the Redis server"
        self.execute_command('PING', callback=callback)

    def save(self, callback=None):
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete
        """
        self.execute_command('SAVE', callback=callback)

    def time(self, callback=None):
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).
        """
        self.execute_command('TIME', callback=callback)

    def shutdown(self, callback=None):
        "Shutdown the server"
        self.execute_command('SHUTDOWN', callback=callback)

    ### BASIC KEY COMMANDS
    def append(self, key, value, callback=None):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.
        """
        self.execute_command('APPEND', key, value, callback=callback)

    def bitcount(self, key, start=None, end=None, callback=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` paramaters indicate which bytes to consider
        """
        args = [a for a in (key, start, end) if a is not None]
        kwargs = {'callback': callback}
        self.execute_command('BITCOUNT', *args, **kwargs)

    def bitop(self, operation, dest, *keys, **kwargs):
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``dest``.
        """
        kwargs = {'callback': kwargs.get('callback', None)}
        self.execute_command('BITOP', operation, dest, *keys, **kwargs)

    def decr(self, name, amount=1, callback=None):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """
        self.execute_command('DECRBY', name, amount, callback=callback)

    def delete(self, *keys, **kwargs):
        "Delete one or more keys specified by ``names``"
        self.execute_command('DEL', *keys, callback=kwargs.get('callback'))

    def dump(self, name, callback=None):
        """
        Return a serialized version of the value stored at the specified key.
        If key does not exist a nil bulk reply is returned.
        """
        self.execute_command('DUMP', name, callback)

    def exists(self, name, callback=None):
        "Returns a boolean indicating whether key ``name`` exists"
        self.execute_command('EXISTS', name, callback=callback)

    def expire(self, name, time, callback=None):
        """
        Set an expire flag on key ``name`` for ``time`` seconds. ``time``
        can be represented by an integer or a Python timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600
        self.execute_command('EXPIRE', name, time, callback=callback)

    def expireat(self, name, when, callback=None):
        """
        Sets an expire flag on ``key``. ``when`` can be represented as an
        integer indicating unix time or a Python datetime.datetime
        object.
        """
        if isinstance(when, datetime.datetime):
            when = int(mod_time.mktime(when.timetuple()))
        self.execute_command('EXPIREAT', name, when, callback=callback)

    def get(self, key, callback=None):
        """
        Return the value at key ``name``, or None if the key doesn't exist
        """
        self.execute_command('GET', key, callback=callback)

    def getbit(self, key, offset, callback=None):
        "Returns a boolean indicating the value of ``offset`` in ``name``"
        self.execute_command('GETBIT', key, offset, callback=callback)

    def getrange(self, name, start, end, callback=None):
        """
        Returns the substring of the string value stored at ``key``,
        determined by the offsets ``start`` and ``end`` (both are inclusive)
        """
        self.execute_command('GETRANGE', name, start, end, callback=callback)

    def getset(self, name, value, callback=None):
        """
        Set the value at key ``name`` to ``value`` if key doesn't exist
        Return the value at key ``name`` atomically
        """
        self.execute_command('GETSET', name, value, callback=callback)

    def incr(self, name, amount=1, callback=None):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        self.execute_command('INCRBY', name, amount, callback=callback)

    def incrbyfloat(self, name, amount=1.0, callback=None):
        """
        Increments the value at key ``name`` by floating ``amount``.
        If no key exists, the value will be initialized as ``amount``
        """
        self.execute_command('INCRBYFLOAT', name, amount, callback=callback)

    def keys(self, pattern='*', callback=None):
        "Returns a list of keys matching ``pattern``"
        self.execute_command('KEYS', pattern, callback=callback)

    def mget(self, *keys, **kwargs):
        """
        Returns a list of values ordered identically to ``keys``
        """
        callback = kwargs.get('callback')
        self.execute_command('MGET', *keys, callback=callback)

    def mset(self, mapping, callback=None):
        """Sets key/values based on a mapping"""
        items = [i for k, v in mapping.iteritems() for i in (k, v)]
        self.execute_command('MSET', *items, callback=callback)

    def msetnx(self, mapping, callback=None):
        """
        Sets key/values based on a mapping if none of the keys are already
        set.
        """
        items = [i for k, v in mapping.iteritems() for i in (k, v)]
        self.execute_command('MSETNX', *items, callback=callback)

    def move(self, key, db, callback=None):
        "Moves the key ``name`` to a different Redis database ``db``"
        self.execute_command('MOVE', key, db, callback=callback)

    def persist(self, name, callback=None):
        "Removes an expiration on ``name``"
        self.execute_command('PERSIST', name, callback=callback)

    def pexpire(self, name, time, callback=None):
        """
        Set an expire flag on key ``key`` for ``time`` milliseconds.
        ``time`` can be represented by an integer or a Python timedelta
        object.
        """
        if isinstance(time, datetime.timedelta):
            ms = int(time.microseconds / 1000)
            time = time.seconds + time.days * 24 * 3600 * 1000 + ms
        self.execute_command('PEXPIRE', name, time, callback=callback)

    def pexpireat(self, name, when, callback=None):
        """
        Set an expire flag on key ``key``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime.datetime object.
        """
        if isinstance(when, datetime.datetime):
            ms = int(when.microsecond / 1000)
            when = int(mod_time.mktime(when.timetuple())) * 1000 + ms
        self.execute_command('PEXPIREAT', name, when, callback=callback)

    def psetex(self, name, time_ms, value, callback=None):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object
        """
        if isinstance(time_ms, datetime.timedelta):
            ms = int(time_ms.microseconds / 1000)
            time_ms = (time_ms.seconds + time_ms.days * 24 * 3600) * 1000 + ms
        self.execute_command('PSETEX', name, time_ms, value, callback=callback)

    def pttl(self, name, callback=None):
        "Returns the number of milliseconds until the key will expire"
        self.execute_command('PTTL', name, callback=callback)

    def randomkey(self, callback=None):
        "Returns the name of a random key"
        self.execute_command('RANDOMKEY', callback=callback)

    def rename(self, src, dst, callback=None):
        """Rename key ``src`` to ``dst``"""
        self.execute_command('RENAME', src, dst, callback=callback)

    def renamenx(self, src, dst, callback=None):
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        self.execute_command('RENAMENX', src, dst, callback=callback)

    def restore(self, name, ttl, value, callback=None):
        """
        Create a key using the provided serialized value, previously obtained
        using DUMP.
        """
        self.execute_command('RESTORE', name, ttl, value, callback=callback)

    def set(self, name, value, ex=None, px=None, nx=False, xx=False,
            callback=None):
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` if it
            does not already exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` if it
            already exists.
        """
        pieces = [name, value]
        if ex:
            pieces.append('EX')
            if isinstance(ex, datetime.timedelta):
                ex = ex.seconds + ex.days * 24 * 3600
            pieces.append(ex)
        if px:
            pieces.append('PX')
            if isinstance(px, datetime.timedelta):
                ms = int(px.microseconds / 1000)
                px = (px.seconds + px.days * 24 * 3600) * 1000 + ms
            pieces.append(px)
        if nx:
            pieces.append('NX')
        if xx:
            pieces.append('XX')
        self.execute_command('SET', *pieces, callback=callback)

    def setbit(self, name, offset, value, callback=None):
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.
        """
        value = value and 1 or 0
        self.execute_command('SETBIT', name, offset, value, callback=callback)

    def setex(self, name, time, value, callback=None):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600
        self.execute_command('SETEX', name, time, value, callback=callback)

    def setnx(self, key, value, callback=None):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"
        self.execute_command('SETNX', key, value, callback=callback)

    def setrange(self, name, offset, value, callback=None):
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.
        """
        self.execute_command(
            'SETRANGE',
            name, offset, value, callback=callback)

    def strlen(self, name, callback=None):
        "Return the number of bytes stored in the value of ``name``"
        self.execute_command('STRLEN', name, callback=callback)

    def substr(self, name, start, end, callback=None):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        self.execute_command('SUBSTR', name, start, end, callback=callback)

    def ttl(self, name, callback=None):
        "Returns the number of seconds until the key ``name`` will expire"
        self.execute_command('TTL', name, callback=callback)

    def type(self, name, callback=None):
        "Returns the type of key ``name``"
        self.execute_command('TYPE', name, callback=callback)

    #### LIST COMMANDS ####
    def blpop(self, keys, timeout=0, callback=None):
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        ks = [keys] if isinstance(keys, basestring) else list(keys)
        ks.append(timeout)
        self.execute_command('BLPOP', *ks, timeout=timeout, callback=callback)

    def brpop(self, keys, timeout=0, callback=None):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        ks = [keys] if isinstance(keys, basestring) else list(keys)
        ks.append(timeout)
        self.execute_command('BRPOP', *ks, timeout=timeout, callback=callback)

    def brpoplpush(self, src, dst, timeout=0, callback=None):
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.
        """
        self.execute_command(
            'BRPOPLPUSH',
            src, dst, timeout, timeout=timeout, callback=callback)

    def lindex(self, name, index, callback=None):
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list
        """
        self.execute_command('LINDEX', name, index, callback=callback)

    def linsert(self, name, where, refvalue, value, callback=None):
        """
        Insert ``value`` in list ``name`` either immediately before or after
        [``where``] ``refvalue``

        Returns the new length of the list on success or -1 if ``refvalue``
        is not in the list.
        """
        self.execute_command(
            'LINSERT',
            name, where, refvalue, value, callback=callback)

    def llen(self, name, callback=None):
        "Return the length of the list ``name``"
        self.execute_command('LLEN', name, callback=callback)

    def lpop(self, name, callback=None):
        "Remove and return the first item of the list ``name``"
        self.execute_command('LPOP', name, callback=callback)

    def lpush(self, name, *values, **kwargs):
        "Push ``values`` onto the head of the list ``name``"
        callback = kwargs.get('callback', None)
        self.execute_command('LPUSH', name, *values, callback=callback)

    def lpushx(self, name, value, callback=None):
        "Push ``value`` onto the head of the list ``name`` if ``name`` exists"
        self.execute_command('LPUSHX', name, value, callback=callback)

    def lrange(self, name, start, end, callback=None):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        self.execute_command('LRANGE', name, start, end, callback=callback)

    def lrem(self, name, value, num=0, callback=None):
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.
        """
        self.execute_command('LREM', name, num, value, callback=callback)

    def lset(self, name, index, value, callback=None):
        "Set ``position`` of list ``name`` to ``value``"
        self.execute_command('LSET', name, index, value, callback=callback)

    def ltrim(self, name, start, end, callback=None):
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        self.execute_command('LTRIM', name, start, end, callback=callback)

    def rpop(self, name, callback=None):
        "Remove and return the last item of the list ``name``"
        self.execute_command('RPOP', name, callback=callback)

    def rpoplpush(self, src, dst, callback=None):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        self.execute_command('RPOPLPUSH', src, dst, callback=callback)

    def rpush(self, name, *values, **kwargs):
        "Push ``values`` onto the tail of the list ``name``"
        callback = kwargs.get('callback', None)
        self.execute_command('RPUSH', name, *values, callback=callback)

    def rpushx(self, name, value, **kwargs):
        "Push ``value`` onto the tail of the list ``name`` if ``name`` exists"
        callback = kwargs.get('callback', None)
        self.execute_command('RPUSHX', name, value, callback=callback)

    def sort(self, name, start=None, num=None, by=None, get=None, desc=False,
             alpha=False, store=None, groups=False, callback=None):
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where int he key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.

        """
        if ((start is not None and num is None) or
                (num is not None and start is None)):
            raise ValueError("``start`` and ``num`` must both be specified")

        pieces = [name]
        if by is not None:
            pieces.append('BY')
            pieces.append(by)
        if start is not None and num is not None:
            pieces.append('LIMIT')
            pieces.append(start)
            pieces.append(num)
        if get is not None:
            if isinstance(get, basestring):
                pieces.append('GET')
                pieces.append(get)
            else:
                for g in get:
                    pieces.append('GET')
                    pieces.append(g)
        if desc:
            pieces.append('DESC')
        if alpha:
            pieces.append('ALPHA')
        if store is not None:
            pieces.append('STORE')
            pieces.append(store)

        if groups:
            if not get or isinstance(get, basestring) or len(get) < 2:
                raise RuntimeError(
                    'when using "groups" the "get" argument '
                    'must be specified and contain at least '
                    'two keys'
                )

        groups = len(get) if groups else None
        self.execute_command('SORT', *pieces, groups=groups, callback=callback)

    #### SCAN COMMANDS ####
    def scan(self, cursor=0, match=None, count=None, callback=None):
        """
        Scan and return (nextcursor, keys)

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        pieces = [cursor]
        if match is not None:
            pieces.extend(['MATCH', match])
        if count is not None:
            pieces.extend(['COUNT', count])
        self.execute_command('SCAN', *pieces, callback=callback)

    def sscan(self, name, cursor=0, match=None, count=None, callback=None):
        """
        Scan and return (nextcursor, members_of_set)

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend(['MATCH', match])
        if count is not None:
            pieces.extend(['COUNT', count])
        self.execute_command('SSCAN', *pieces, callback=callback)

    def hscan(self, name, cursor=0, match=None, count=None, callback=None):
        """
        Scan and return (nextcursor, dict)

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend(['MATCH', match])
        if count is not None:
            pieces.extend(['COUNT', count])
        self.execute_command('HSCAN', *pieces, callback=callback)

    def zscan(self, name, cursor=0, match=None, count=None,
              score_cast_func=float, callback=None):
        """
        Scan and return (nextcursor, pairs)

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend(['MATCH', match])
        if count is not None:
            pieces.extend(['COUNT', count])
        self.execute_command(
            'ZSCAN',
            *pieces, score_cast_func=score_cast_func, callback=callback)

    #### SET COMMANDS ####
    def sadd(self, name, *values, **kwargs):
        "Add ``value(s)`` to set ``name``"
        callback = kwargs.get('callback', None)
        self.execute_command('SADD', name, *values, callback=callback)

    def scard(self, name, callback=None):
        "Return the number of elements in set ``name``"
        self.execute_command('SCARD', name, callback=callback)

    def sdiff(self, keys, callback=None):
        "Return the difference of sets specified by ``keys``"
        self.execute_command('SDIFF', *keys, callback=callback)

    def sdiffstore(self, dest, keys, callback=None):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        self.execute_command('SDIFFSTORE', dest, *keys, callback=callback)

    def sinter(self, keys, callback=None):
        "Return the intersection of sets specified by ``keys``"
        self.execute_command('SINTER', *keys, callback=callback)

    def sinterstore(self, dest, keys, callback=None):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        self.execute_command('SINTERSTORE', dest, *keys, callback=callback)

    def sismember(self, name, value, callback=None):
        "Return a boolean indicating if ``value`` is a member of set ``name``"
        self.execute_command('SISMEMBER', name, value, callback=callback)

    def smembers(self, name, callback=None):
        "Return all members of the set ``name``"
        self.execute_command('SMEMBERS', name, callback=callback)

    def smove(self, src, dst, value, callback=None):
        "Move ``value`` from set ``src`` to set ``dst`` atomically"
        self.execute_command('SMOVE', src, dst, value, callback=callback)

    def spop(self, name, callback=None):
        "Remove and return a random member of set ``name``"
        self.execute_command('SPOP', name, callback=callback)

    def srandmember(self, name, number=None, callback=None):
        """
        If ``number`` is None, returns a random member of set ``name``.

        If ``number`` is supplied, returns a list of ``number`` random
        memebers of set ``name``. Note this is only available when running
        Redis 2.6+.
        """
        args = number and [number] or []
        self.execute_command('SRANDMEMBER', name, *args, callback=callback)

    def srem(self, name, *values, **kwargs):
        "Return the union of sets specified by ``keys``"
        callback = kwargs.get('callback', None)
        self.execute_command('SREM', name, *values, callback=callback)

    def sunion(self, keys, callback=None):
        "Return the union of sets specified by ``keys``"
        self.execute_command('SUNION', *keys, callback=callback)

    def sunionstore(self, dest, keys, callback=None):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        self.execute_command('SUNIONSTORE', dest, *keys, callback=callback)

    ### SORTED SET COMMANDS

    def zadd(self, name, *args, **kwargs):
        """
        Set any number of score, element-name pairs to the key ``name``. Pairs
        can be specified in two ways:

        As *args, in the form of: score1, name1, score2, name2, ...
        or as **kwargs, in the form of: name1=score1, name2=score2, ...

        The following example would add four values to the 'my-key' key:
        redis.zadd('my-key', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        """
        callback = None
        if 'callback' in kwargs:
            callback = kwargs.pop('callback')

        pieces = []
        if args:
            if len(args) % 2 != 0:
                raise RuntimeError("Equal number of vals and scores required")
            pieces.extend(args)
        for pair in kwargs.iteritems():
            pieces.append(pair[1])
            pieces.append(pair[0])
        # invke
        self.execute_command('ZADD', name, *pieces, callback=callback)

    def zcard(self, name, callback=None):
        "Return the number of elements in the sorted set ``name``"
        self.execute_command('ZCARD', name, callback=callback)

    def zcount(self, name, min, max, callback=None):
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.
        """
        self.execute_command('ZCOUNT', name, min, max, callback=callback)

    def zincrby(self, name, value, amount, callback=None):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        self.execute_command('ZINCRBY', name, amount, value, callback=callback)

    def zinterstore(self, dest, keys, aggregate=None, callback=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        self._zaggregate('ZINTERSTORE', dest, keys, aggregate, callback)

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float, callback=None):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results descendingly

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        if desc:
            self.zrevrange(name, start, end, withscores,
                           score_cast_func, callback)
            return

        pieces = ['ZRANGE', name, start, end]
        if withscores:
            pieces.append('withscores')
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func,
            'callback': callback
        }
        self.execute_command(*pieces, **options)

    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float, callback=None):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")
        pieces = ['ZRANGEBYSCORE', name, min, max]
        if start is not None and num is not None:
            pieces.extend(['LIMIT', start, num])
        if withscores:
            pieces.append('withscores')
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func,
            'callback': callback
        }
        self.execute_command(*pieces, **options)

    def zrank(self, name, value, callback=None):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        self.execute_command('ZRANK', name, value, callback=callback)

    def zrem(self, name, *values, **kwargs):
        "Remove member ``values`` from sorted set ``name``"
        callback = kwargs.get('callback', None)
        self.execute_command('ZREM', name, *values, callback=callback)

    def zremrangebyrank(self, name, min, max, callback=None):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """
        self.execute_command('ZREMRANGEBYRANK', name, min, max,
                             callback=callback)

    def zremrangebyscore(self, name, min, max, callback=None):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        self.execute_command('ZREMRANGEBYSCORE', name, min, max,
                             callback=callback)

    def zrevrange(self, name, start, end, withscores=False,
                  score_cast_func=float,  callback=None):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces = ['ZREVRANGE', name, start, end]
        if withscores:
            pieces.append('withscores')
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func,
            'callback': callback
        }
        self.execute_command(*pieces, **options)

    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float,
                         callback=None):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")
        pieces = ['ZREVRANGEBYSCORE', name, max, min]
        if start is not None and num is not None:
            pieces.extend(['LIMIT', start, num])
        if withscores:
            pieces.append('withscores')
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func,
            'callback': callback
        }
        self.execute_command(*pieces, **options)

    def zrevrank(self, name, value, callback=None):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        self.execute_command('ZREVRANK', name, value, callback=callback)

    def zscore(self, name, value, callback=None):
        "Return the score of element ``value`` in sorted set ``name``"
        self.execute_command('ZSCORE', name, value, callback=callback)

    def zunionstore(self, dest, keys, aggregate=None, callback=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        self._zaggregate('ZUNIONSTORE', dest, keys, aggregate, callback)

    def _zaggregate(self, command, dest, keys, aggregate, callback):
        pieces = [command, dest, len(keys)]
        if isinstance(keys, dict):
            keys, weights = keys.iterkeys(), keys.itervalues()
        else:
            weights = None
        pieces.extend(keys)
        if weights:
            pieces.append('WEIGHTS')
            pieces.extend(weights)
        if aggregate:
            pieces.append('AGGREGATE')
            pieces.append(aggregate)
        return self.execute_command(*pieces, callback=callback)

    ### HASH COMMANDS
    def hdel(self, name, *keys, **kwargs):
        "Delete ``keys`` from hash ``name``"
        callback = kwargs.get('callback')
        self.execute_command('HDEL', name, *keys, callback=callback)

    def hexists(self, name, key, callback=None):
        "Returns a boolean indicating if ``key`` exists within hash ``name``"
        self.execute_command('HEXISTS', name, key, callback=callback)

    def hget(self, name, key, callback=None):
        "Return the value of ``key`` within the hash ``name``"
        self.execute_command('HGET', name, key, callback=callback)

    def hgetall(self, name, callback=None):
        "Return a Python dict of the hash's name/value pairs"
        self.execute_command('HGETALL', name, callback=callback)

    def hincrby(self, name, key, amount=1, callback=None):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"
        self.execute_command('HINCRBY', name, key, amount, callback=callback)

    def hincrbyfloat(self, name, key, amount=1.0, callback=None):
        """
        Increment the value of ``key`` in hash ``name`` by floating ``amount``
        """
        self.execute_command('HINCRBYFLOAT', name, key, amount,
                             callback=callback)

    def hkeys(self, name, callback=None):
        "Return the list of keys within hash ``name``"
        self.execute_command('HKEYS', name, callback=callback)

    def hlen(self, name, callback=None):
        "Return the number of elements in hash ``name``"
        self.execute_command('HLEN', name, callback=callback)

    def hset(self, name, key, value, callback=None):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        self.execute_command('HSET', name, key, value, callback=callback)

    def hsetnx(self, name, key, value, callback=None):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        self.execute_command('HSETNX', name, key, value, callback=callback)

    def hmset(self, name, mapping, callback=None):
        """
        Set key to value within hash ``name`` for each corresponding
        key and value from the ``mapping`` dict.
        """
        items = [i for k, v in mapping.iteritems() for i in (k, v)]
        self.execute_command('HMSET', name, *items, callback=callback)

    def hmget(self, name, keys, callback=None):
        "Returns a list of values ordered identically to ``keys``"
        self.execute_command('HMGET', name, *keys, callback=callback)

    def hvals(self, name, callback=None):
        "Return the list of values within hash ``name``"
        self.execute_command('HVALS', name, callback=callback)

    def publish(self, channel, message, callback=None):
        """
        Publish ``message`` on ``channel``.
        Returns the number of subscribers the message was delivered to.
        """
        self.execute_command('PUBLISH', channel, message, callback=callback)

    def eval(self, script, numkeys, *keys_and_args, **kwargs):
        """
        Execute the Lua ``script``, specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        callback = kwargs.get('callback')
        self.execute_command(
            'EVAL',
            script, numkeys, *keys_and_args, callback=callback)

    def evalsha(self, sha, numkeys, *keys_and_args, **kwargs):
        """
        Use the ``sha`` to execute a Lua script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        callback = kwargs.get('callback')
        self.execute_command(
            'EVALSHA',
            sha, numkeys, *keys_and_args,  callback=callback)

    def script_exists(self, *args, **kwargs):
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as ``args``. Returns a list of boolean values indicating if
        if each already script exists in the cache.
        """
        options = {'parse': 'EXISTS', 'callback': kwargs.get('callback')}
        self.execute_command('SCRIPT', 'EXISTS', *args, **options)

    def script_flush(self, callback=None):
        "Flush all scripts from the script cache"
        options = {'parse': 'FLUSH', 'callback': callback}
        self.execute_command('SCRIPT', 'FLUSH', **options)

    def script_kill(self, callback=None):
        "Kill the currently executing Lua script"
        options = {'parse': 'KILL', 'callback': callback}
        self.execute_command('SCRIPT', 'KILL', **options)

    def script_load(self, script, callback=None):
        "Load a Lua ``script`` into the script cache. Returns the SHA."
        options = {'parse': 'LOAD', 'callback': callback}
        return self.execute_command('SCRIPT', 'LOAD', script, **options)
