#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. __init__.py

Tools for Eurostat data collections upload

**About**

*credits*:      `grazzja <jacopo.grazzini@ec.europa.eu>`_ 

*version*:      0.1
--
*since*:        Wed Jan  4 02:25:43 2017


**Description**

**Usage**

    >>> import eurobase
    
"""


__all__ = ['settings', 'session', 'collection', 'api']#analysis:ignore

#==============================================================================
# PROGRAM METADATA
#==============================================================================

metadata = dict([
                ('project', 'esdata'),
                ('date', 'Wed Dec  7 23:52:39 2016'),
                ('url', 'https://github.com/gjacopo/eurobase'),
                ('organization', 'European Union'),
                ('license', 'European Union Public Licence (EUPL)'),
                ('version', '0.1'),
                ('description', 'Tools for data collections upload from Eurostat website'),
                ('credits',  ['grazzja']),
                ('contact', 'jacopo.grazzini@ec.europa.eu'),
                ])

#==============================================================================
# GLOBAL CLASSES/METHODS/VARIABLES
#==============================================================================

class EurobaseError(Exception):
    """Base class for exceptions in this module."""
    def __init__(self, msg, expr=None):    
        self.msg = msg
        if expr is not None:    self.expr = expr
    def __str__(self):              return repr(self.msg)
class EurobaseWarning(Warning):
    """Base class for warnings in this module."""
    def __init__(self, msg, expr=None):    
        self.msg = msg
        if expr is not None:    self.expr = expr
    def __str__(self):              return repr(self.msg)
