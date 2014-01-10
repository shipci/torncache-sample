# -*- mode: python; coding: utf-8 -*-

# Copyright (c) 2012 Yummy Bian <yummy.bian#gmail.com>.

"""
Random. Legacy sharding distribution algorithm
"""

from __future__ import absolute_import

import bisect
import hashlib

# local requirements
from torncache.distributions import Distribution


class Ketama(Distribution):
    """A Ketama distribution mechanism"""

    def add_nodes(self, objects):
        objects = objects or {}
        self.nodes.extend(objects.keys())
        self.weights.update(objects.copy())

        # Generates the ring.
        for node in self.nodes[self.index:]:
            self.total_weight += self.weights.get(node, 1)

        for node in self.nodes[self.index:]:
            weight = 1
            if node in self.weights:
                weight = self.weights.get(node)
            factor = 40 * weight
            for j in xrange(0, int(factor)):
                b_key = self._hash_digest('%s-%s' % (node, j))
                for i in xrange(0, 3):
                    key = self._hash_val(b_key, lambda x: x+i*4)
                    self.key_node[key] = node
                    self.keys.append(key)

        self.index = len(self.nodes)
        self.keys.sort()

    def del_nodes(self, nodes):
        nodes = nodes or []
        # Delete nodes from the ring.
        for node in nodes:
            weight = 1
            if node in self.weights:
                weight = self.weights.get(node)
            factor = 40 * weight
            for j in xrange(0, int(factor)):
                b_key = self._hash_digest('%s-%s' % (node, j))
                for i in xrange(0, 3):
                    key = self._hash_val(b_key, lambda x: x+i*4)
                    self.keys.remove(key)
                    del self.key_node[key]
            self.index -= 1
            self.nodes.remove(node)

    def get_node(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned.

        If the hash ring is empty, `None` is returned.
        """
        pos = self.get_node_pos(string_key)
        if pos is None:
            return None
        return self.key_node[self.keys[pos]]

    def get_node_pos(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned
        along with it's position in the ring.

        If the hash ring is empty, (`None`, `None`) is returned.
        """
        if not self.key_node:
            return None
        key = self.gen_key(string_key)
        nodes = self.keys
        pos = bisect.bisect(nodes, key)
        if pos == len(nodes):
            return 0
        else:
            return pos

    def iterate_nodes(self, string_key, distinct=True):
        """Given a string key it returns the nodes as a generator that can
        hold the key.

        The generator iterates one time through the ring
        starting at the correct position.

        if `distinct` is set, then the nodes returned will be unique,
        i.e. no virtual copies will be returned.

        """
        if not self.key_node:
            yield None, None

        returned_values = set()

        def distinct_filter(value):
            if str(value) not in returned_values:
                returned_values.add(str(value))
                return value

        pos = self.get_node_pos(string_key)

        for key in self.keys[pos:]:
            val = distinct_filter(self.key_node[key])
            if val:
                yield val

        for i, key in enumerate(self.keys):
            if i < pos:
                val = distinct_filter(self.key_node[key])
                if val:
                    yield val

    def gen_key(self, key):
        """Given a string key it returns a long value,
        this long value represents a place on the hash ring.

        md5 is currently used because it mixes well.
        """
        b_key = self._hash_digest(super(Ketama, self).gen_key(key))
        return self._hash_val(b_key, lambda x: x)

    def clear(self):
        """Reset contents"""
        self.keys = []
        self.key_node = {}
        self.nodes = []
        self.index = 0
        self.weights = {}
        self.total_weight = 0

    def _hash_val(self, b_key, entry_fn):
        """Imagine keys from 0 to 2^32 mapping to a ring,
        so we divide 4 bytes of 16 bytes md5 into a group.
        """
        return ((b_key[entry_fn(3)] << 24)
                | (b_key[entry_fn(2)] << 16)
                | (b_key[entry_fn(1)] << 8)
                | b_key[entry_fn(0)])

    def _hash_digest(self, key):
        m = hashlib.md5()
        m.update(key.encode('utf-8'))
        return map(ord, m.digest())
