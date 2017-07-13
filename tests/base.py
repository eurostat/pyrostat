#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
..  _tests_base

Utility functions for eurobase unit test module 

Description
-----------

Dependencies
------------

*require*:      unittest, os, sys, re, warnings, time

About
-----

*credits*:      `grazzja <jacopo.grazzini@jrc.ec.europa.eu>`_ 

*since*:        Tue Jun  6 14:15:37 2017

*version*:      0.1
"""


#==============================================================================
# PROGRAM METADATA
#==============================================================================


#==============================================================================
# IMPORT STATEMENTS
#==============================================================================

import os, sys #analysis:ignore
import unittest
import re#analysis:ignore
import datetime, time#analysis:ignore


#==============================================================================
# GLOBAL VARIABLES/METHODS
#==============================================================================

#==============================================================================
# METHODS
#==============================================================================

#/****************************************************************************/
def __runonetest(testCase, **kwargs):
    try:
        t_class = testCase.__name__
        t_module = testCase.__module__
    except: raise IOError('unexpected input testing class entity')
    else:
        t_module_basename = t_module.split('.')[-1]        
    try:
        t_submodule = testCase.module
    except: raise IOError('unrecognised tested submodule')
    message = ''
    #message = '\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
    message += '\n{}: Test {}.py module of eurobase.{}'
    print(message.format(t_class, t_submodule, t_module_basename))
    #warnings.warn()
    verbosity = kwargs.pop('verbosity',2)
    suite = unittest.TestLoader().loadTestsFromTestCase(testCase)
    unittest.TextTestRunner(verbosity=verbosity).run(suite)
    return

#/****************************************************************************/
def runtest(*TestCases, **kwargs):
    if len(TestCases)==0:                       
        return
    for testCase in TestCases:
        __runonetest(testCase, **kwargs)
        if len(TestCases)>1 and testCase!=TestCases[-1]: 
            time.sleep(1)
    return



