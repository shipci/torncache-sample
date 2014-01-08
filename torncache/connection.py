# -*- mode: python; coding: utf-8 -*-

"""
Torncache Connection
"""

from __future__ import absolute_import

import os
import stat
import socket
import time
import numbers
import logging
import functools

from tornado import iostream
from tornado import stack_context
from tornado.ioloop import IOLoop
from tornado.platform.auto import set_close_exec


class ConnectionError(Exception):
    "Base exception class"


class ConnectionTimeoutError(ConnectionError):
    """Timeout when connecting or running and operation"""


class Connection(object):
    """ A Client connection to a Server"""

    def __init__(self, host, ioloop=None, serializer=None, deserializer=None,
                 connect_timeout=5, timeout=1, no_delay=True, ignore_exc=False,
                 dead_retry=30):

        # Parse host conf and weight
        self.weight = 1
        if isinstance(host, tuple):
            host, self.weight = host

        # By default, assume unix socket
        self.ip, self.port, self.path = None, None, host
        # Parse host port
        if ":" in host:
            self.ip, _, self.port = host.partition(":")
            self.port = int(self.port)
            self.path = None

        # Check that it's a valid path to an unix socket. fail otherwise
        if self.path and not os.path.exists(self.path):
            raise ConnectionError("Invalid unix socket '%s'".format(self.path))

        # Protected data
        self._ioloop = ioloop or IOLoop.instance()
        self._ignore_exc = ignore_exc

        # Timeouts
        self._timeout = None
        self._request_timeout = timeout
        self._connect_timeout = connect_timeout

        # Data
        self._serializer = serializer
        self._deserializer = deserializer

        # Connections properites
        self._stream = None
        self._no_delay = no_delay
        self._dead_until = 0
        self._dead_retry = dead_retry
        self._connect_callbacks = []

    def __str__(self):
        retval = "%s:%d" % (self.ip, self.port)
        if self._dead_until:
            retval += " (dead until %d)" % self._dead_until
        return retval

    def __del__(self):
        self.close()

    def _add_timeout(self, reason, timeout=None):
        """Add a timeout handler"""
        def on_timeout():
            self._timeout = None
            self.mark_dead(reason)
            raise ConnectionTimeoutError(reason)

        # Allow to override default timeout per call
        if not isinstance(timeout, numbers.Integral):
            timeout = self._request_timeout

        if timeout:
            self._clear_timeout()
            self._timeout = self._ioloop.add_timeout(
                time.time() + timeout,
                stack_context.wrap(on_timeout))

    def _clear_timeout(self):
        if self._timeout is not None:
            self._ioloop.remove_timeout(self._timeout)
            self._timeout = None

    def mark_dead(self, reason):
        """Quarintine MC server for a period of time"""
        if self._dead_until < time.time():
            logging.warning("Marking dead %s: '%s'" % (self, reason))
            self._dead_until = time.time() + self._dead_retry
            self._clear_timeout()
            self.close()

    def connect(self, callback=None):
        """Open a connection to MC server"""

        def on_timeout(reason):
            self._timeout = None
            self.mark_dead(reason)
            raise ConnectionTimeoutError(reason)

        def on_close():
            self._clear_timeout()
            if self._stream and self._stream.error:
                error = self._stream.error
                self._stream = None
                if self._connect_callbacks:
                    self._connect_callbacks = None
                    raise error
                logging.error(self._stream.error)

        def on_connect():
            self._clear_timeout()
            for callback in self._connect_callbacks:
                callback and callback(self)
            self._connect_callbacks = None

        # Check if server is dead
        if self._dead_until > time.time():
            msg = "Server {0} will stay dead next {1} secs"
            msg = msg.format(self, self._dead_until - time.time())
            raise ConnectionError(msg)
        self._dead_until = 0

        # Check we are already connected
        if self._connect_callbacks is None:
            callback and callback(self)
            return
        self._connect_callbacks.append(callback)
        if self._stream and not self._stream.closed():
            return

        # Connection closed. clean and start again
        self.close()

        # Set timeout
        if self._connect_timeout:
            timeout_func = functools.partial(on_timeout, "Connection Timeout")
            self._timeout = self._ioloop.add_timeout(
                time.time() + self._connect_timeout,
                stack_context.wrap(timeout_func))

        # now connect to host...
        if self.path is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self._no_delay:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            connect = (self.ip, self.port)
        # or unix socket
        else:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            set_close_exec(sock.fileno())
            st = os.stat(self.path)
            if not stat.S_ISSOCK(st.st_mode):
                raise ValueError("File %s exists and is not a socket", file)
            connect = self.path

        self._stream = iostream.IOStream(sock, io_loop=self._ioloop)
        self._stream.set_close_callback(on_close)
        self._stream.connect(connect, callback=on_connect)

    def send(self, cmd, callback):
        """Send a MC command"""
        self._stream.write(cmd + "\r\n", callback)

    def write(self, data, callback):
        """Write operation"""
        self._stream.write(data, callback)

    def read(self, rlen=None, callback=None):
        """Read operation"""
        on_response = lambda x: callback(x[:-2])
        if rlen is None:
            self._stream.read_until("\r\n", on_response)
        else:
            # Read and strip CRLF
            rlen = rlen + 2  # CRLF
            self._stream.read_bytes(rlen, on_response)

    def readline(self, callback):
        """Read a line"""
        self._stream.read_until("\r\n", callback)

    def close(self):
        """Close connection to MC"""
        self._stream and self._stream.close()

    def closed(self):
        """Check connection status"""
        if not self._stream:
            return True
        return self._stream and self._stream.closed()
