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

PACKAGE         = "esdata"

API_DOMAIN      = 'ec.europa.eu/eurostat/wdds/rest/data'
"""
Domain of Eurostat API.
"""
API_VERS        = 2.1
"""
Version of Eurostat API.
"""
API_PRECISION   = 1 # only available at the moment? 
"""
Precision of data fetched through Eurostat API. 
"""
API_FMTS        = ('json', 'unicode')
"""
Formats supported by Eurostat API. 
"""
API_LANGS       = ('en','de','fr')
"""
Languages supported by Eurostat API.
"""

PROTOCOLS       = ('http', 'https', 'ftp')
"""
Recognised protocols (API, bulk downloads,...).
"""
DEF_PROTOCOL    = 'http'
"""
Default protocol used by the API.
"""
LANGS           = ('en','de','fr')
"""
Languages supported by this package.
"""
DEF_LANG        = 'en'
"""
Default language used when launching Eurostat API.
"""
DEF_SORT        = 1
"""
Default sort value.
"""
DEF_FMT        = 'json'
"""
Default format of data returned by Eurostat API request.
"""

BULK_DOMAIN     = 'ec.europa.eu/eurostat/estat-navtree-portlet-prod/'
"""
Online repository of bulk Eurostat datasets.
"""
BULK_QUERY      = 'BulkDownloadListing'
"""
Address linking to bulk datasets.
"""
BULK_DIC_DIR    = 'dic'
"""
Directory (address) of bulk dictionaries.
"""
BULK_DIC_LIST   = 'dimlist'
"""
Code for dim/list data.
"""
BULK_DIC_EXTS   = ['dic'] # should be a list
"""
Extension ("format") of bulk dictionaries.
"""
BULK_DIC_NAMES  = ['Name', 'Size', 'Type', 'Date']
"""
Labels used in the tables informing the bulk dictionaries.
"""
BULK_DIC_ZIP    = 'gz' 
"""
Extension ("format") of bulk dictionaries.
"""
BULK_DATA_DIR   = 'data'
"""
Directory (address) of bulk datasets.
"""
BULK_DATA_EXTS  = ['tsv', 'sdmx'] # should be a list
"""
Extensions ("formats") of bulk datasets.
"""
BULK_DATA_NAMES  = ['Name', 'Size', 'Type', 'Date']
"""
Labels used in the tables informing the bulk datasets.
"""
BULK_DATA_ZIP   = 'gz'
"""
Extension ("format") of compressed bulk datasets. 
"""
BULK_BASE_FILE  = 'metabase'
"""
Name of the base file storing all metadata about Eurostat datasets.
"""
BULK_BASE_EXT   = 'txt'
"""
Extension ("format") of the base file.
"""
BULK_BASE_ZIP   = 'gz' 
"""
Extension ("format") of compressed base file (if compressed). 
"""
BULK_BASE_NAMES = ['data', 'dic', 'label'] # should be a list
"""
Labels used in the tables informing the base file.
""" # note: DO NOT MODIFY ... or also modify Collection.__get_member accordingly
BULK_TOC_FILE   = 'table_of_contents'
"""
Name of the table of contents providing contents of Eurostat database.
"""
BULK_TOC_EXTS   = ['txt', 'xml'] # should be a list
"""
Extensions ("formats") of table of contents.
"""
BULK_TOC_ZIP    = ''
"""
Extension ("format") of compressed table of contents. 
"""
BULK_TOC_NAMES  = ['title', 'code', 'type',                                 \
                   'last update of data', 'last table structure change',    \
                   'data start', 'data end']
"""
Labels used in the table of contents.
"""
KW_DEFAULT      = 'default'
"""
"""

BS_PARSERS      = ("html.parser","html5lib","lxml","xml")
"""
"""

EXCEPTIONS          = {}
