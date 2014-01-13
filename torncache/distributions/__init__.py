# -*- mode: python; coding: utf-8 -*-

from __future__ import absolute_import

import re

__all__ = ['Distribution']


class Distribution(object):

    def __init__(self, nodes=None, hash_tags=None):
        # Htags support
        self.htags_re = None
        self.htags_template = None
        self.htags = hash_tags

        # Compute Htags helper
        if hash_tags and len(hash_tags) == 2:
            self.htags_re = hash_tags[0] + '(?P<key>.*)}' + hash_tags[1]
            self.htags_re = re.compile(self.htags_re)
            self.htags_template = hash_tags[0] + '{0}' + hash_tags[1]

        # clear and add nodes
        self.clear()
        self.add_nodes(nodes)

    def __del__(self):
        self.clear()

    def add_nodes(self, nodes):
        raise NotImplementedError

    def del_nodes(self, nodes):
        raise NotImplementedError

    def iterate_nodes(self, key, distinct=True):
        yield None

    def gen_key(self, key):
        match = None
        if self.htags_re:
            match = self.htags_re.match(key)
        return match.groups('key') if match else key

    def tag_key(self, key):
        if self.htags_template:
            return self.tags.template.format(key)
        return key

    def untag_key(self, key):
        return self.gen_key(key)

    def clear(self):
        raise NotImplementedError

    @staticmethod
    def create(name, *args, **kwargs):
        if name == 'ketama':
            from torncache.distributions.ketama import Ketama
            return Ketama(*args, **kwargs)
        if name == 'random' or name is None:
            from torncache.distributions.random import Random
            return Random(*args, **kwargs)
        # catch all
        raise ImportError("Unknown distribution {0}".format(name))
