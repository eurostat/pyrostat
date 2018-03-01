#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. __init__.py

Tools for Eurostat data collections upload.

**About**

*credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_ 

*version*:      0.1
--
*since*:        Wed Jan  4 02:25:43 2017


**Description**

**Usage**

    >>> import pyrostat
    
"""

__all__ = ['settings', 'session', 'collection', 'api']#analysis:ignore

#==============================================================================
# PROGRAM METADATA
#==============================================================================

__project__     = 'pyroStat'
__url__         = 'https://github.com/gjacopo/eurobase'
__organization__ = 'European Commission (EC - DG ESTAT)'
# note that neither __project__ nor __url__ ar __organization__ are special/protected 
# variables in python language, unlike the other fields below
__description__ = 'Tools for data collections upload from Eurostat website'
__package__     = __project__.lower() # already set in fact...
__date__        = '2017'
__author__      = 'Jacopo Grazzini'
__contact__     = 'jacopo.grazzini@ec.europa.eu'
__license__     = 'European Union Public Licence (EUPL)'
__version__     = '0.1'
__copyright__   = 'European Union'
                                   
#==============================================================================
# CORE DEFINITION
#==============================================================================

#/****************************************************************************/
# dummy __metadata class to retrieve easily the metadata of the software.
#/****************************************************************************/
class __metadata(dict):

    __metadata_keys = ['project', 'description', 'url', 'package', 'subpackage', \
                       'author', 'contact', 'license', 'copyright', 'organization', \
                       'credits', 'date', 'version']
    __lenght_longest_key = max([len(k) for k in __metadata_keys])
                        
    #/************************************************************************/
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.__dict__ = self
 
    #/************************************************************************/
    def copy(self, *args, **kwargs):
        return self.__class__(**self.__dict__)

    #/************************************************************************/
    def __repr__(self):
        return "<{} instance at {}>".format(self.__class__.__name__, id(self))
    def __str__(self):    
        l = self.__lenght_longest_key
        return "\n".join(["{} : {}".format(k.ljust(l),getattr(self,k))
            for k in self.__metadata_keys if self.get(k) not in ('',None)])    

    #/************************************************************************/
    def __getattr__(self, attr):
        if attr.startswith('__'):
            try:        nattr = attr[2:-2]
            except:     nattr = None
        else:
            nattr = attr
        if nattr in self.keys():  
            r = self.get(nattr)
        else:
            try:        object.__getattribute__(self, attr) 
            except:     pass
            r = None
        return r
        
#/****************************************************************************/
# here we go: we will use this variable outside the metadata module
metadata = __metadata({'project'     : __project__,
                       'description' : __description__,
                       'package'     : __package__,
                       'version'     : __version__,
                       'author'      : __author__,
                       'contact'     : __contact__,
                       'license'     : __license__,
                       'copyright'   : __copyright__,
                       'organization':__organization__,
                       'url'         : __url__,
                       'date'        : '',
                       'credits'     : '',
                       'subpackage'  : ''
                       })                                                        

