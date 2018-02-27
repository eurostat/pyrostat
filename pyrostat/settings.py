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


from collections import OrderedDict

#==============================================================================
# GLOBAL CLASSES/METHODS/VARIABLES
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

