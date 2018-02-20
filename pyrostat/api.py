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


from . import ESDataWarning, ESDataError
from . import settings
from .session import Session
from .collection import Collection

#==============================================================================
# CLASSES/METHODS
#==============================================================================
    
class API(object):
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
    def __init__(self, **kwargs):
        # set default values
        self._domain = self.HOST_DOMAIN
        self._vers = self.VERS_REST
        self._fmt = self.FMT_REST
        self._lang = self.DEF_LANG
        self._precision = self.DEF_PRECISION
        self._dataset = ''
        self._filters = {}
        self._url = None
        self._status = None
        self._force_check = False
        # check whether any argument is passed
        if kwargs == {}:
            return
        # update
        attrs = ('domain','dataset','filters','fmt','lang','vers','force_check') 
        for attr in list(set(attrs).intersection(kwargs.keys())):
            try:
                setattr(self, '{}'.format(attr), kwargs.pop(attr))
            except: 
                warnings.warn(ESDataWarning('wrong attribute value {}'.format(attr.upper())))

    #/************************************************************************/
    def __call__(self, **kwargs):
        """Call method for attributes' updating. 
        
            >>> req(**kwargs) 
            
        Keyword Arguments
        -----------------
        kwargs : 
            keyword argument with keys accepted by :meth:`setDomain`, :meth:`setURL`,
            :meth:`setFormat`, :meth:`setVersion`, and :meth:`setDataset`\ .                     
        """
        params={}
        for attr in ('domain','fmt','lang','vers','dataset','precision','filters'):
            #try:    
                Get = getattr(self, 'get{attr}'.format(attr=attr.title()))
            #except: 
                params.update({attr: Get(kwargs.get(attr))}) 
            #else:   
            #    pass
        if kwargs != {}: # possible filters arguments passed without the 'filters' keyword
            params['filters'].update(kwargs)
        url = self.getUrl(self, **params)
        status, resp = self.fetch(url=url)
        return status, resp

    #/************************************************************************/
    def __repr__(self):
        """Generic representation special method: print the class name of a :class:`{Request}` 
        instance.
        """
        return "<{} instance at {}>".format(self.__class__.__name__, id(self))
    def __str__(self): 
        """Generic string printing method: print all the non :literal:`None` parameters
        and other attributes of a :class:`{Request}` instance.
        """
        obj_str = ''
        args = [('Dataset', 'dataset'), ('URL', 'url'), ('domain', 'domain'), ('Filters', 'filters'),   \
            ('Precision', 'precision'), ('Format', 'fmt'), ('Language', 'lang'), ('Version', 'vers'),   \
            ('Status', 'status')]
        disp_field = lambda title: title # +'\n' + '-'*len(title)
        disp_attribute = lambda attr:                                                               \
            "{val}".format(val=getattr(self,attr))  if not isinstance(getattr(self,attr), dict)   \
            else reduce(lambda x,y:x+y, ["{k}:{v}\t".format(k=k,v=v)                              \
            for (k,v) in getattr(self,attr).items() if v is not None])
        for (k,v) in args: #args.items():
            obj_str += disp_field('{title}:'.format(title=k))
            try:  
                tmp_str = disp_attribute('{content}'.format(content=v))
                assert tmp_str != ''
            except: 
                obj_str += '\t[!not set!]\n'
            else:
                obj_str += '\t' + tmp_str + '\n'
        return obj_str    

    #/************************************************************************/
    @property
    def fmt(self):
        return self.__fmt
    @fmt.setter
    def fmt(self, fmt):
        if not isinstance(fmt, str):
            raise ESDataError('wrong type for FMT parameter')
        elif not lang in settings.API_FMTS:
            raise ESDataError('format not supported')
        self.__fmt = fmt
           
    #/************************************************************************/
    @property
    def lang(self):
        return self.__lang
    @lang.setter
    def lang(self, lang):
        if not isinstance(lang, str):
            raise ESDataError('wrong type for LANG parameter')
        elif not lang in settings.API_LANGS:
            raise ESDataError('language not supported')
        self.__lang = lang
      
    #/************************************************************************/
    @property
    def vers(self):
        return self.__vers     
    @vers.setter
    def vers(self,version):
        if not(version is None or isinstance(version,float)): 
            raise ESDataError('wrong format/unrecognised version')
        elif version < settings.API_VERSION:           
            raise ESDataError('version not supported') 
        self.__vers = version
      
    #/************************************************************************/
    @property
    def domain(self):
        return self.__domain     
    @domain.setter
    def domain(self,domain):
        if not(domain is None or isinstance(domain,str)):    
            raise ESDataError('wrong format/unrecognised domain') 
        self.__domain = domain
      
    #/************************************************************************/
    @staticmethod
    def _check_dataset(dataset):
        if not(dataset is None or isinstance(dataset,str)):   
            raise ESDataError('wrong format/unrecognised dataset') 
        # CHECK DIMENSIONS!!!
        # elif dataset < VERS_REST:            raise ESDataError('version not supported')
        else:                               
            return dataset
    def setDataset(self, dataset):
        self._dataset=self._check_dataset(dataset)       
    def getDataset(self, dataset=None):
        return self._check_dataset(dataset) or self._dataset
    @property
    def dataset(self):
        return self._dataset     
    @dataset.setter
    def dataset(self,dataset):
        self.setDataset(dataset)

    #/************************************************************************/
    @staticmethod
    def _check_precision(precision):
        if not(precision is None or isinstance(precision, int)):    
            raise ESDataError('wrong format/unrecognised precision')
        else:                                   
            return precision
    def setPrecision(self, precision):
        self._precision=self._check_precision(precision)       
    def getPrecision(self, precision=None):
        return self._check_precision(precision) or self._precision
    @property
    def precision(self):
        return self._precision      
    @precision.setter
    def precision(self,precision):
        self.setPrecision(precision)

    #/************************************************************************/
    @staticmethod
    def _check_filters(**kwargs):
        if kwargs == {}:
            return None # ' '
        # filters = '&'.join(['{k}={v}'.format(k=k, v=v) for k, v in kwargs.items()])
        # filters="?{filters}".format(filters=filters)
        if 'filters' in kwargs:
            return kwargs.pop('filters')
        else:
            return kwargs
    def setFilters(self, **kwargs):
        """Create a bunch of filters to be used for request *Eurobase* web service.
        """
        self._filters=self._check_filters(**kwargs)       
    def getFilters(self, dummy=None, **kwargs):
        """Create a bunch of filters to be used for request *Eurobase* web service.
        """
        return self._check_filters(**kwargs) or self._filters
    @property
    def filters(self):
        return self._filters    
    @filters.setter
    def filters(self, d):        
        self._filters.update(d)

    #/************************************************************************/
    @staticmethod
    def _get_status(status):
        if status is None:                      raise ESDataError('unknown status')
        elif not isinstance(status, int):       raise ESDataError('unrecognised status')
        else:                                   return status
    def setStatus(self, status):
        self._status=self._get_status(status)       
    def getStatus(self, status=None):
        return status or self._status
    @property
    def status(self):
        return self._status      
    @status.setter
    def status(self,status):
        self.setStatus(status)

    #/************************************************************************/
    @staticmethod
    def _get_url(**kwargs):
        """Create the query URL to *Eurobase* web service.
        
            >>> url = x._get_url(**kwargs)
           
        Keyword Arguments
        -----------------
        kwargs : dict
            define the parameters for web service.
                
        Returns
        -------
        url : str
            link to Eurobase web service to submit the specified query.

        Note
        ----
        The generic url formatting is:
            {host_url}/rest/data/{version}/{format}/{language}/{datasetCode}?{filters}
    
        Example: 
            http://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/ilc_li03?precision=1&indic_il=LI_R_MD60&time=2015
        """
        if kwargs == {}:
            return None
        elif not('domain' in kwargs and 'vers' in kwargs and 'lang' in kwargs and 'fmt' in kwargs):
            raise ESDataError('uncomplete information for building URL')
        # set parameters
        vers=kwargs.pop('vers') 
        fmt=kwargs.pop('fmt') 
        lang=kwargs.pop('lang')
        kwargs.update({'path': "v{vers}/{fmt}/{lang}".format(vers=vers,fmt=fmt,lang=lang)}) 
        if 'precision' not in kwargs:   
            kwargs.update({'precision': 1})
        url = Session.build_url(**kwargs)
        return url
    def setURL(self, **kwargs):
        [kwargs.update({attr: kwargs.get(attr) or getattr(self, '_{attr}'.format(attr=attr))})
            for attr in ('domain','fmt','lang','vers','dataset','precision','filters')]
        self._url=self._get_url(**kwargs)
    def getURL(self, dummy=None, **kwargs):
        # update/merge passed arguments with already existing ones
        [kwargs.update({attr: kwargs.get(attr) or getattr(self, '_{attr}'.format(attr=attr))})
            for attr in ('domain','fmt','lang','vers','dataset','precision','filters')]
        # actually return
        return self._get_url(**kwargs) or self._url
    @property
    def url(self):
        return self._url

    #/************************************************************************/
    def set(self, **kwargs):
        if kwargs.get(settings.KW_DEFAULT) is True:  
            kwargs = {}
        elif kwargs == {}:
            return
        # get/update
        tmpkw=kwargs
        #try:
        [setattr(self, attr, kwargs.get(attr))                          \
            for attr in tmpkw                                          \
            if attr in ('domain','fmt','lang','vers','dataset','precision','filters')]
        #except: 
        #    pass # raise IOError, 'Request properties not (re)set'
        try:            
            self.setUrl(**kwargs)
        except:
            raise ESDataError('Request URL not (re)set')
    def get(self):
        """Get/retrieve the stored parameters: dataset, url (in this order).
        
            >>> dataset, url = x.get()
            
        See also
        --------
        :data:`dataset`, :data:`url`
        """
        return [getattr(self, '_{attr}'.format(attr=attr), None)       \
                for attr in ('dataset', 'url')]
    def clear(self):
        """Clear the main parameters of the current :class:`{Request}` instance.
        """
        try:    
            [setattr(self, '_{attr}'.format(attr=attr), None)       \
                for attr in ('domain','fmt','lang','vers','dataset','precision','url','filters','status')]
        except: 
            pass
      
                
    #/************************************************************************/
    @staticmethod                                        
    def _decode_json(response):
        json_data = json.loads(response)
        if 'error' in json_data:
            return json_data["error"]
        return json_data

    #/************************************************************************/
    def fetch(self, url=None):
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
            raise ESDataError('error reading URL')    
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
                    raise ESDataError('method/attribute {} not implemented'.format(attr))
       """
       pass
      
      
#==============================================================================
# MAIN METHOD AND TESTING AREA
#==============================================================================
def main():
    """esdata.py main()"""
    return
    
if __name__ == '__main__':
    main()
    
