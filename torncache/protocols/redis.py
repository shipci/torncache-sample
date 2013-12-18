# -*- mode: python; coding: utf-8 -*-

"""
Protocol. A Base class to implement cached protocols
"""

from __future__ import absolute_import

import itertools
from tornado.gen import engine, Task

# redis deps
import hiredis

# Local requirements
from torncache.protocol import ProtocolMixin
from torncache.connection import Connection

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
            from redis.client import StrictRedis
            self.response_callbacks = StrictRedis.RESPONSE_CALLBACKS

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

        try:
            # Open connection if required
            conn.closed() and (yield Task(conn.connect))
            # Add timeout for this request
            conn._add_timeout("Timeout on command '{0}'".format(cmd))
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

    def ping(self, callback=None):
        "Ping the Redis server"
        self.execute_command('PING', callback=callback)
