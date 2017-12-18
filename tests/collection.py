#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
About
-----

*credits*:      `gjacopo <jacopo.grazzini@jrc.ec.europa.eu>`_ 

*since*:        Tue Jun  6 13:54:22 2017
"""

#==============================================================================
# IMPORT STATEMENTS
#==============================================================================

import unittest
import warnings
import datetime
import math
import re

from esdata import settings, collection
from . import runtest as baseRuntest

#/****************************************************************************/
# LocationTestCase
#/****************************************************************************/
class CollectionTestCase(unittest.TestCase):
    """Class of tests for `collection.py`
    """    
    module = 'estat'
    
    #/************************************************************************/
    def setUp(self):
        self.domain       = settings.BULK_DOMAIN
        self.lang         = settings.DEF_LANG
        self.sort         = settings.DEF_SORT
        self.query        = settings.BULK_QUERY
        self.Some_Collection = collection.Collection(domain=self.domain, lang=self.lang,
                                                     sort=self.sort, query=self.query)
        
    #/************************************************************************/
    def test1_set(self):
        self.assertEqual(self.Some_Collection.domain, self.domain)
        self.assertEqual(self.Some_Collection.lang, self.lang)
        self.assertEqual(self.Some_Collection.sort, self.sort)
        self.assertEqual(self.Some_Collection.query, self.query)
        

        
#/****************************************************************************/
# MAIN METHOD AND TESTING AREA
#/****************************************************************************/

def runtest():
    baseRuntest(CollectionTestCase)
    return
    
if __name__ == '__main__':
    unittest.main()
