# -*- mode: python; coding: utf-8 -*-

"""
Random. Legacy sharding distribution algorithm
"""

from __future__ import absolute_import

import itertools


class Random(object):
    """Random distribution"""

    def __init__(self, nodes=None, hash_tags=None):
        self.htags = hash_tags
        # clear and add nodes
        self.clear()
        self.add_nodes(nodes)

    def __del__(self):
        self.close()

    def add_nodes(self, nodes):
        nodes = nodes or {}
        for name, weight in nodes.iteritems():
            for i in xrange(weight):
                self._buckets.append(name)

    def del_nodes(self, nodes):
        self._buckets = [node for node in self._buckets if node not in nodes]

    def get_node(self, key):
        pos = self.get_node_pos(key)
        return self._buckets[pos] if pos else None

    def get_node_pos(self, key):
        if self._buckets:
            return self.gen_key(key) % len(self._buckets)
        raise IndexError(key)

    def iterate_nodes(self, key, distinct=True):

        def dfilter(value):
            if value not in visited:
                visited.add(value)
                return True
            return False

        if self._buckets:
            visited = set()
            currpos = self.get_node_pos(key)
            for node in itertools.ifilter(dfilter, self._buckets[currpos:]):
                yield node
            ffunc = lambda x: x[0] < currpos and dfilter(x[1])
            for i,node in itertools.ifilter(ffunc, enumerate(self._buckets)):
                yield node

        yield None

    def gen_key(self, key):
        return hash(key)

    def clear(self):
        # connection options
        self._buckets = []
