# -*- mode: python; coding: utf-8 -*-

from __future__ import absolute_import


def find(name):
    if name == 'ketama':
        from torncache.distributions.ketama import Ketama
        return Ketama
    if name == 'random' or name is None:
        from torncache.distributions.random import Random
        return Random
    # catch all
    raise ImportError("Unknown distribution {0}".format(name))
