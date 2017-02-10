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



#==============================================================================
# GLOBAL CLASSES/METHODS/VARIABLES
#==============================================================================

PACKAGE         = "esdata"

PROTOCOLS       = ('http', 'https', 'ftp')
"""
"""
DEF_PROTOCOL    = 'http'
"""
"""
LANGS           = ('en','de','fr')
"""
"""
DEF_LANG        = 'en'
"""
"""
DEF_SORT        = 1
"""
"""
"""
"""
DEF_FMT        = 'json'
"""
"""

BULK_DOMAIN     = 'ec.europa.eu/eurostat/estat-navtree-portlet-prod/'
"""
"""
BULK_QUERY      = 'BulkDownloadListing'
"""
"""
BULK_DIC_DIR    = 'dic'
"""
"""
BULK_DIC_LIST   = 'dimlist'
"""
"""
BULK_DIC_EXTS   = ['dic'] # should be a list
"""
"""
BULK_DIC_NAMES  = {'Name':'Name', 'Size':'Size', 'Type':'Type', 'Date':'Date'}
"""
"""
BULK_DIC_ZIP    = 'gz' 
"""
"""
BULK_DATA_DIR   = 'data'
"""
"""
BULK_DATA_EXTS  = ['tsv', 'sdmx'] # should be a list
"""
"""
BULK_DATA_NAMES  = {'Name':'Name', 'Size':'Size', 'Type':'Type', 'Date':'Date'}
"""
"""
BULK_DATA_ZIP   = 'gz'
"""
"""
BULK_BASE_FILE  = 'metabase'
"""
"""
BULK_BASE_EXT   = 'txt'
"""
"""
BULK_BASE_ZIP   = 'gz' 
"""
"""
BULK_BASE_NAMES = {'data':'dataset', 'dic':'dimension', 'label':'label'} # should be a list
"""
""" # note: DO NOT MODIFY ... or also modify Collection.__get_member accordingly
BULK_TOC_FILE   = 'table_of_contents'
"""
"""
BULK_TOC_EXTS   = ['txt', 'xml'] # should be a list
"""
"""
BULK_TOC_ZIP    = ''
"""
"""
BULK_TOC_NAMES  = ['title','code','type','last update of data',     \
                   'last table structure change',                   \
                   'data start','data end']


API_DOMAIN      = 'ec.europa.eu/eurostat/wdds/rest/data'
"""
"""
API_VERS        = 2.1
"""
"""
API_PRECISION   = 1 # only available at the moment? 
"""
"""
API_FMTS        = ('json', 'unicode')
API_LANGS       = ('en','de','fr')
KW_DEFAULT      = 'default'
"""
"""

BS_PARSERS      = ("html.parser","html5lib","lxml","xml")
"""
"""

EXCEPTIONS          = {}