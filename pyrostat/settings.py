#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. settings.py

Basic definitions for Eurobase API

**About**

*credits*:      `grazzja <jacopo.grazzini@ec.europa.eu>`_ 

*version*:      0.1
--
*since*:        Tue Jan  3 23:52:40 2017

**Contents**
"""

import os, sys
import inspect
from collections import OrderedDict, Mapping
import logging

from pyrostat import metadata

#==============================================================================
# GLOBAL VARIABLES
#==============================================================================

PACKAGE             = "pyrostat"

PROTOCOLS           = ('http', 'https', 'ftp')
"""
Recognised protocols (API, bulk downloads,...).
"""
DEF_PROTOCOL        = 'http'
PROTOCOL            = DEF_PROTOCOL
"""
Default protocol used by the API.
"""
LANGS               = ('en','de','fr')
"""
Languages supported by this package.
"""
DEF_LANG            = 'en'
"""
Default language used when launching Eurostat API.
"""

EC_URL              = 'ec.europa.eu'
"""
European Commission URL.
"""
ESTAT_DOMAIN        = 'eurostat' 
"""
Eurostat domain under European Commission URL.
"""
# ESTAT_URL           = '%s://%s/%s' % (PROTOCOL, EC_URL, ESTAT_DOMAIN)
ESTAT_URL           = '%s/%s' % (EC_URL, ESTAT_DOMAIN)
"""
Eurostat complete URL.
"""

API_SUBDOMAIN       = 'wdds/rest/data'
"""
Subdomain of Eurostat API.
"""
API_DOMAIN          = '%s/%s' % (ESTAT_URL, API_SUBDOMAIN)
"""
Domain of Eurostat API.
"""
API_VERS            = 2.1
"""
Version of Eurostat API.
"""
API_PRECISION       = 1 # only available at the moment? 
"""
Precision of data fetched through Eurostat API. 
"""
API_FMTS            = ('json', 'unicode')
"""
Formats supported by Eurostat API. 
"""
API_LANGS           = ('en','de','fr')
"""
Languages supported by Eurostat API.
"""

DEF_SORT            = 1
"""
Default sort value.
"""
DEF_FMT             = 'json'
"""
Default format of data returned by Eurostat API request.
"""

BULK_SUBDOMAIN      = 'estat-navtree-portlet-prod'
"""
Subdomain of the repository for bulk Eurostat datasets.
"""
BULK_DOMAIN         = '%s/%s' % (ESTAT_URL, BULK_SUBDOMAIN)
"""
Online repository for bulk Eurostat datasets.
"""
BULK_QUERY          = 'BulkDownloadListing'
"""
Address linking to bulk datasets.
"""
BULK_DIR            = {'dic':   'dic', 
                       'data':  'data', 
                       'base':  '',
                       'toc':   ''}
"""
Directory (address) of bulk dictionaries/datasets/metadata files.
"""
BULK_LIST           = {'dic':   'dimlist', 
                       'data':  '', 
                       'base':  '',
                       'toc':   ''}
"""
Code for dim/list data.
"""
BULK_EXTS           = {'dic':   ['dic',], 
                       'data':  ['tsv', 'sdmx'], 
                       'base':  ['txt',],
                       'toc':   ['txt', 'xml']}
"""
Extension ("format") of bulk dictionaries/datasets/metadata files.
"""
BULK_NAMES          =  {'dic':  {'name': 'Name', 'size':'Size', 'type':'Type', 'date':'Date'},
                        'data': {'name': 'Name', 'size':'Size', 'type':'Type', 'date':'Date'},
                        'base': {'data':'data', 'dic':'dic', 'label':'label'},
                        'toc':  {'title':'title', 'code':'code', 'type':'type', \
                                 'last_update':'last update of data',           \
                                 'last_change': 'last table structure change',  \
                                 'start':'data start', 'end':'data end'}}
"""
Labels used in the tables informing the bulk dictionaries/datasets/metadata files.
"""
BULK_ZIP            = {'dic':   'gz', 
                       'data':  'gz', 
                       'base':  'gz',
                       'toc':   ''}
"""
Extension ("format") of compressed bulk dictionaries/datasets/metadata files.
"""
BULK_FILES          = {'dic':   '', 
                       'data':  '', 
                       'base':  'metabase',
                       'toc':   'table_of_contents'}
"""
Generic string used for naming the bulk dictionaries/datasets/metadata files, for
instance the file storing all metadata about Eurostat datasets, or the the table 
of contents providing contents of Eurostat database.
"""

KW_DEFAULT          = 'default'
"""
"""

BS_PARSERS          = ("html.parser", "html5lib", "lxml", "xml")
"""
"""

EXCEPTIONS          = {}

LEVELS              = {'debug': logging.DEBUG,
                       'info': logging.INFO,
                       'warning': logging.WARNING,
                       'error': logging.ERROR,#     
                       'critical': logging.CRITICAL}
"""Levels of warning/errors; default level is 'debug'."""

LOG_FILENAME            = metadata.package + '.log'
"""Log file name: where warning/info messages will be output."""

#==============================================================================
# ERROR/WARNING CLASSES
#==============================================================================

class pyroError(Exception):
    """Base class for exceptions in this module."""
    def __init__(self, msg, expr=None):    
        self.msg = msg
        if expr is not None:    self.expr = expr
        Exception.__init__(self, msg)
    def __str__(self):              return repr(self.msg)

class pyroWarning(Warning):
    """Base class for warnings in this module."""
    def __init__(self, msg, expr=None):    
        self.msg = msg
        if expr is not None:    self.expr = expr
        # logging.warning(self.msg)
    def __repr__(self):             
        return self.msg
    def __str__(self):              return repr(self.msg)
    
#==============================================================================
# LOGGER CLASS
#==============================================================================
    
class pyroLogger(object): 
    """Basic logger class.
    """  
    def __init__(self, **kwargs):    
        self.logger = logging.getLogger() #'logging_kinki
        if not self.logger.handlers: 
            filename = kwargs.pop('filename',LOG_FILENAME)
            self.logger.addHandler(logging.FileHandler(filename))
            self.logger.setLevel(LEVELS[kwargs.pop('level','debug')])   
    def close(self):    
        for handler in self.logger.handlers[:]:
            try:    handler.close() # FileHandler
            except: handler.flush() # StreamHandler
            self.logger.removeHandler(handler)
    def __getattr__(self, method):
        try:    return getattr(logging,method)
        except: pass
    
LOGGER = pyroLogger()
"""Logger object: where warning/info operations are defined."""

#==============================================================================
# GLOBAL CLASSES/METHODS/VARIABLES
#==============================================================================

def fileexists(file):
    """Check file existence.
    """
    return os.path.exists(os.path.abspath(file))

def clean_key_method(kwargs, method):
    """Clean keyword parameters prior to be passed to a given method/function by
    deleting all the keys that are not present in the signature of the method/function.
    """
    parameters = inspect.signature(method).parameters
    keys = [key for key in kwargs.keys()                                          \
            if key not in list(parameters.keys()) or parameters[key].KEYWORD_ONLY.value==0]
    [kwargs.pop(key) for key in keys]
    return kwargs

def to_key_val_list(value):
    """Take an object and test to see if it can be represented as a
    dictionary. If it can be, return a list of tuples, e.g.,

        >>> to_key_val_list([('key', 'val')])
        [('key', 'val')]
        >>> to_key_val_list({'key': 'val'})
        [('key', 'val')]
        >>> to_key_val_list('string')
        ValueError: cannot encode objects that are not 2-tuples.
    """
    if value is None:
        return None     
    elif isinstance(value, (str, bytes, bool, int)):
        raise ValueError('cannot encode objects that are not 2-tuples')     
    elif isinstance(value, Mapping):
        value = value.items()     
    return list(value)

def merge_dict(dnew, dold, dict_class=OrderedDict):
    """Determine appropriate setting for a given request, taking into account
    the explicit setting on that request, and the setting in the session. If a
    setting is a dictionary, they will be merged together using `dict_class`
    """
    if dold is None:
        return dnew
    elif dnew is None:
        return dold
    elif not (isinstance(dold, Mapping) and isinstance(dnew, Mapping)):
        return dnew
    merged_dict = dict_class(to_key_val_list(dold))
    merged_dict.update(to_key_val_list(dnew))
    # remove keys that are set to None. Extract keys first to avoid altering
    # the dictionary during iteration.
    none_keys = [k for (k, v) in merged_dict.items() if v is None]
    for key in none_keys:
        del merged_dict[key]
    return merged_dict
