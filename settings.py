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
BULK_DIC_EXT    = 'dic'
"""
"""
BULK_DATA_DIR   = 'data'
"""
"""
BULK_DATA_EXT   = 'tsv.gz'
"""
"""
BULK_BASE_FILE  = 'metabase'
"""
"""
BULK_BASE_EXT   = 'txt.gz'
"""
"""
BULK_BASE_NAMES = ['dataset', 'dimension', 'label']
"""
"""
# note: DO NOT MODIFY ... or also modify Bulkdata.__get_member accordingly

API_DOMAIN      = 'ec.europa.eu/eurostat/wdds/rest/data'
"""
"""
API_VERS        = 2.1
"""
"""
API_PRECISION   = 1 # only available at the moment? 
"""
"""

KW_DEFAULT      = 'default'
"""
"""

BS_PARSERS      = ("html.parser","html5lib","lxml","xml")
"""
"""
