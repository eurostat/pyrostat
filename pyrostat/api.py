#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. api.py

Tools for data collections download from 
`Eurostat online database <http://ec.europa.eu/eurostat>`_
together with search and manipulation utilities.

**About**

*credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_ 

*version*:      0.1
--
*since*:        Wed Dec  7 23:52:39 2016

**Description**

**Usage**

    >>> <put_here_an_example>
    
**Dependencies**

*call*:         :mod:`settings`, :mod:`request`, :mod:`collections`

*require*:      <put_here_required_modules>
                :mod:`os`, :mod:`sys`, :mod:`string`, :mod:`inspect`, :mod:`warnings`, \ 
                :mod:`re`, :mod:`math`, :mod:`operator`, :mod:`itertools`, :mod:`collections`
                
*optional*:     <put_here_optional_modules>
                :mod:`numpy`, :mod:`scipy`, :mod:`matplotlib`, :mod:`pylab`,                        \
                :mod:`pickle`, :mod:`cPickle`

**See also**
https://github.com/ropengov/eurostat
                
**Contents**
"""


#==============================================================================
# IMPORT STATEMENTS
#==============================================================================

import os, sys 
import inspect, warnings
import string, re, time
import operator, itertools, collections

    
try:                                
    import simplejson as json
except ImportError:          
    import json
else:
    pass

from functools import reduce

#==============================================================================
# GLOBAL CLASSES/METHODS/VARIABLES
#==============================================================================


from . import pyroWarning, pyroError
from . import settings
from session import Session
from collection import Collection

#==============================================================================
# CLASSES/METHODS
#==============================================================================
    

class Query(object):
    pass 

class Eurostat(object):
    """
    {host_url}/rest/data/{version}/{format}/{language}/{datasetCode}?{filters}

    host_url : fixed part of the request related to our website
    service : fixed part of the request related to the service  
    version : fixed part of the request related to the version of the service
    format :  data format to be returned (json or unicode)  
    language : language used for metadata (en/fr/de)

    note
    ---- 
    see <http://ec.europa.eu/eurostat/web/json-and-unicode-web-services/getting-started/rest-request>_.
    
    Example of errors:
    {"error":{"status":"400","label":"Bad Request Error. Message: Dataset code is invalid."}}
    {"error":{"status":"400","label":"Dataset contains no data. One or more filtering elements (query parameters) are probably invalid."}}
    """
      
                
    #/************************************************************************/
    @staticmethod                                        
    def _decode_json(response):
        json_data = json.loads(response)
        if 'error' in json_data:
            return json_data["error"]
        return json_data

    #/************************************************************************/
    def fetch(self, url=None):
       pass
       """Fetch data from (well formed) URL to *Eurobase* 
        
            >>> resp = x.fetch_data(url)
                
        Arguments
        ---------
        url : str
            link to Eurobase web service to submit the specified query, e.g. the URL
            output by :meth:`get_url`\ .
        
        Keyword Arguments
        -----------------
        fmt : bool 
            set to :literal:`True`, when the output is read as a JSON response;
            defaults to :literal:`False`\ .
            
        Returns
        -------
        resp : dict
            dictionary.
            
        Errors
        ------
        * {"error":{"status":"416","label":"Too many categories have been requested. Maximum is 50."}}
        * {"error":{"status":"400","label":"Invalid value for 'wsVersion' parameter"}}
         if url is None or url=='':
            url=self.url
        self.status, response = Session.get_data(url, fmt=self.fmt)
        try:
            resp = response.read()
        except:
            raise pyroError('error reading URL')    
        if fmt == 'json':
            resp = json.loads(resp)
        elif fmt == 'unicode':
            resp = resp # TODO
        return self.status, resp
      
        def __getattr__(self, attr):
            if attr.startswith('__'):  # to avoid some bug of the pylint editor
                try:        return object.__getattribute__(self, attr) 
                except:     pass 
            # try:        return object.__getattribute__(self.__call__, attr) 
            # except:     pass 
            # finally, what we are really interested in
            try:        
                return getattr(jsonstat, attr)
            except:     
                try:        
                    return getattr(pyjstat, attr)
                except:     
                    raise pyroError('method/attribute {} not implemented'.format(attr))
       """
      
      
#==============================================================================
# MAIN METHOD AND TESTING AREA
#==============================================================================
def main():
    """esdata.py main()"""
    return
    
if __name__ == '__main__':
    main()
    
