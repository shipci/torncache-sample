from __future__ import absolute_import

import logging


def __import(mod, names=None):
    """
    Names are imported from mod immediately, and then added to the
    global scope.  (It is equivalent to 'from mod import <names>')
    """
    # No lazy importing, import everything immediately.
    try:
        omod = __import__(mod, globals(), fromlist=names)
        if names:
            # from mod import <names>
            for name in names:
                globals()[name] = getattr(omod, name)
        else:
            # import mod
            globals()[mod] = omod
    except Exception as err:
        logging.warning("Ignored protocol '{0}': {1}".format(mod, str(err)))


def __find(path, expr="*.py*"):
    """
    return a tuple of valid modules form __import__
    """
    import os
    import glob
    import itertools

    index, pattern = 0, os.path.join(path, expr)
    if not os.path.isabs(path):
        path = os.path.dirname(__file__)
        index = len(path)
        pattern = os.path.abspath(os.path.join(path, pattern))

    func = lambda x: x[:x.rfind('.')].replace(os.sep, '.')[index + 1:]
    modules = set(itertools.imap(func, glob.glob(pattern)))
    # remove '__init__' if present
    "__init__" in modules and modules.remove("__init__")
    return modules

#pylint: disable-msg=W0141
map(__import, __find(".",  expr="*.py*"))
