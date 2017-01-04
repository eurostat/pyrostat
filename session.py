#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. session.py

Basic class for common request operations 

**About**

*credits*:      `grazzja <jacopo.grazzini@ec.europa.eu>`_ 

*version*:      0.1
--
*since*:        Wed Jan  4 01:49:11 2017

**Description**

**Usage**

    >>> <put_here_an_example>
    
**Dependencies**

*call*:         :mod:`settings`

*require*:      <put_here_required_modules>
                :mod:`os`, :mod:`sys`, :mod:`string`, :mod:`inspect`, :mod:`warnings`, \ 
                :mod:`re`, :mod:`math`, :mod:`operator`, :mod:`itertools`, :mod:`collections`
                
*optional*:     <put_here_optional_modules>
                :mod:`pickle`, :mod:`cPickle`

**Contents**
"""
  
#==============================================================================
# IMPORT STATEMENTS
#==============================================================================

import os, sys 
import inspect, warnings
import string, re, time
   
try:                                
    import requests # urllib2
except ImportError:                 
    raise IOError

try:                                
    import requests_cache # https://pypi.python.org/pypi/requests-cache
except ImportError:          
    pass
    
# Beautiful soup package
try:                                
    import bs4
except ImportError:                 
    pass
   
try:                                
    import simplejson as json
except ImportError:          
    import json
else:
    pass

try:
    import pandas as pd
except ImportError:          
    class pandas:
        def read_table(*args, **kwargs): 
            raise IOError

try: # Python doesn't pickle method instance by default
    import pickle, cPickle #analysis:ignore   
    import copy_reg, types
    def _pickle_method(m): # identify the fields of the method
        return _unpickle_method, (m.im_func.__name__, m.im_self, m.im_class)
    def _unpickle_method(func_name, obj, cls):
        for cls in cls.mro():
            try:                    func = cls.__dict__[func_name]
            except KeyError:        pass
            else:                   break
        return func.__get__(obj, cls)
    # http://matthewrocklin.com/blog/work/2013/12/05/Parallelism-and-Serialization/
    copy_reg.pickle(types.MethodType, _pickle_method, _unpickle_method)
except ImportError:
    pass

#==============================================================================
# GLOBAL CLASSES/METHODS/VARIABLES
#==============================================================================

import settings


#==============================================================================
# CLASSES/METHODS
#==============================================================================

class Session(object):
    """
    """
    
    def __init__(self, **kwargs):
        self.__session      = requests.session()
        self.__cache        = None
        self.__expire       = 0 # datetime.deltatime(0)
        self.__force_download   = False
    
    """
    class _defaultErrorHandler(urllib2.HTTPDefaultErrorHandler):
        def http_error_default(self, req, fp, code, msg, headers):
            results = urllib2.HTTPError(req.get_full_url(), code, msg, headers, fp)
            results.status = code
            return results
        
    class _redirectHandler(urllib2.HTTPRedirectHandler):
        def __init__ (self):
            self.redirects = []
        def http_error_301(self, req, fp, code, msg, headers):
            results = urllib2.HTTPRedirectHandler.http_error_301(self, req, fp, code, msg, headers)
            results.status = code
            return results    
        def http_error_302(self, req, fp, code, msg, headers):
            results = urllib2.HTTPRedirectHandler.http_error_302(self, req, fp, code, msg, headers)
            results.status = code
            return results
    
    class _headRequest(urllib2.Request):
        def get_method(self):
            return "HEAD"
        
    """

    #/************************************************************************/
    @property
    def cache(self):
        return self.__cache
    @cache.setter
    def cache(self, cache):
        if not isinstance(cache, str):
            raise EurobaseError('wrong type for CACHE parameter')
        self.__cache = os.path.abspath(cache)

    #/************************************************************************/
    @property
    def expire(self):
        return self.__expire
    @expire.setter
    def expire(self, expire):
        if not isinstance(expire, (int, datetime.timedelta)):
            raise EurobaseError('wrong type for EXPIRE parameter')
        elif isinstance(expire, int) and expire<0:
            raise EurobaseError('wrong setting for EXPIRE parameter')
        self.__expire = expire

    #/************************************************************************/
    @property
    def force_download(self):
        return self.__force_download
    @force_download.setter
    def force_download(self, force_download):
        if not isinstance(force_download, bool):
            raise EurobaseError('wrong type for FORCE_DOWNLOAD parameter')
        self.__force_download = force_download

    #/************************************************************************/
    @property
    def session(self):
        return self.__session
        
    #/************************************************************************/
    @classmethod
    def build_url(cls, domain, **kwargs):
        """Create a query URL to *Eurobase* web service.
        
            >>> url = Requests.build_url(domain, **kwargs)
           
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
        # retrieve parameters/build url
        url = domain.strip("/")
        if 'protocol' in kwargs:    protocol = kwargs.pop('protocol')
        else:                       protocol = settings.DEF_PROTOCOL
        if protocol not in settings.PROTOCOLS:
            raise EurobaseError('web protocol not recognised')
        if not url.startswith(protocol):  
            url = "{protocol}://{url}".format(protocol=protocol, url=url)
        if 'path' in kwargs:      
            url = "{url}/{path}".format(url=url,path=kwargs.pop('path'))
        if 'query' in kwargs:      
            url = "{url}/{query}".format(url=url,query=kwargs.pop('query'))
        if kwargs != {}:
            _izip_replicate = lambda d : [[(k,i) for i in d[k]] if isinstance(d[k], (tuple,list))        \
                else (k, d[k])  for k in d]          
            #def _izip_replicate(d):
            #    for k in d:
            #        if isinstance(d[k], (tuple,list)):
            #            for i in d[k]: yield (k,i)
            #        else:
            #            yield(k, d[k])
            #print urlencode(kwargs)
            filters = '&'.join(['{k}={v}'.format(k=k, v=v) for (k, v) in _izip_replicate(kwargs)])
            # filters = '&'.join(map("=".join,kwargs.items()))
            url = "{url}?{filters}".format(url=url, filters=filters)
        return url
    
    #/************************************************************************/
    def get_status(self, session, url):
        """Download just the header of a URL and return the server's status code.
        
            >>> session = Session()
            >>> status = session.get_status(url)
            
        Arguments
        ---------

        Returns
        -------

        Raises
        ------

        Note
        ----
        Here is what do status codes mean (taken from wikipedia, <https://en.wikipedia.org/wiki/List_of_HTTP_status_codes>):
    
        1xx - informational
        2xx - success
        3xx - redirection
        4xx - client error
        5xx - server error
                
        Examples
        --------
        >>> 
        
        See also
        --------
        """        
        try:
             response = self.__session.head(url)
             response.raise_for_status()
        except:
             raise EurobaseError('wrong request formulated')  
        else:
             status = response.status_code
             response.close()
        ## urllib2 variant
        # request = urllib2.Request(url)
        # request.get_method = lambda : 'HEAD'
        # try:
        #     response = urllib2.urlopen(request)
        # except urllib2.HTTPError as e:
        #     status, msg = cls.__decode_error(e)
        #     raise EurobaseError('Error {} : {}'.format(status, msg))  
        # else:
        #     status = response.getcode()
        #     response.close()
        return status # in requests.codes.ok ?
          
    #/************************************************************************/
    def get_response(self, url):
        """
        
            >>> session = Session()
            >>> response = session.get_response(url)
            
        Arguments
        ---------

        Returns
        -------

        Raises
        ------

        Note
        ----
                
        Examples
        --------
        >>> 
        
        See also
        --------
        """
        try:
             response = self.__session.get(url) # self.__session.request('get',url)
             response.raise_for_status()
        except:
             raise EurobaseError('wrong request formulated')  
        ## urllib2 variant
        # request = urllib2.Request(url)
        # try:
        #     response = urllib2.urlopen(request)
        # except urllib2.HTTPError as e:
        #     status, msg = cls.__decode_error(e)
        #     raise EurobaseError('Error {} : {}'.format(status, msg))  
        return response   
                        
    #/************************************************************************/
    @staticmethod
    def __decode_error(error):
        status, msg = error.code, error.read()
        try:    
            msg=json.loads( msg )
        except: 
            pass
        else:
            try:    status=msg['error']['status']
            except: pass
            try:    msg=msg['error']['label']
            except: pass
        return status, msg
            
    #/************************************************************************/
    @staticmethod
    def __build_pathname(cls, filename, dirname=None):
        pathname = filename.encode('utf-8')
        try:
            pathname = hashlib.md5(pathname).hexdigest()
        except:
            pathname = pathname.hex()
        if dirname is not None and dirname != '':
            pathname = os.path.join(dirname, pathname)
        return pathname
                        
    #/************************************************************************/
    def fetch_html(self, url, pathname, cache=None, expire=0, force_donwload=False):
        """Download url from internet.
        Store the downloaded content into <cache_dir>/file.
        If <cache_dir>/file exists, it returns content from disk
        :param url: page to be downloaded
        :param filename: filename where to store the content of url, None if we want not store
        :param time_to_live: how many seconds to store file on disk,
            None use default time_to_live,
                                 0 don't use cached version if any
        :returns: the content of url (str type)
        """
        # create cache directory only the fist time it is needed
        if not os.path.exists(cache):
            os.makedirs(cache)
        if not os.path.isdir(cache):
            raise EurobaseError('cache {} is not a directory'.format(cache))
        # note: html must be a str type not byte type
        if force_download is True or expire == 0 or not cls.is_cached(pathname):
            response = self.__session.get_response(url)
            html = response.text
            self.write_to_cache(pathname, html)
        else:
            html = self.read_from_cache(pathname)
        return html
    
    #/************************************************************************/
    def write_to_cache(self, pathname, content):
        """write content to pathname
        :param pathname:
        :param content:
        """
        f = open(pathname, 'w')
        f.write(content)
        f.close()
    
    def read_from_cache(self, pathname):
        """it reads content from pathname
        :param pathname:
        """
        f = open(pathname, 'r')
        content = f.read()
        f.close()
        return content
        
    #/************************************************************************/
    def is_cached(self, pathname, expire=0):
        if not os.path.exists(pathname):
            return False
        elif expire is None or expire==0:
            return True
        else:
            cur = time.time()
            mtime = os.stat(pathname).st_mtime
            # print("last modified: %s" % time.ctime(mtime))
            return cur - mtime < expire
       
    #/************************************************************************/
    @classmethod
    def read_html_table(cls, html, **kwargs): # read vegetables
        """
        
            >>> 
            
        Arguments
        ---------
        
        Keyword Arguments
        -----------------        

        Returns
        -------

        Raises
        ------

        Note
        ----
                
        Examples
        --------
        >>> 
        
        See also
        --------
        """
        parser = kwargs.get('kwargs','html.parser') 
        if parser not in ('html.parser','html5lib','lxml'):
            raise EurobaseError('unknown soup parser')
        ## urllib2 variant
        # html = response.read()
        try:
            raw = bs4.BeautifulSoup(html, parser)
            #raw = bs4.BeautifulSoup(html, parser).get_text()
        except:
            raise EurobaseError('impossible to read HTML page') 
        try:
            tables = raw.findAll('table', **kwargs)
        except:
            raise EurobaseError('error with soup from HTML page')
        headers, rows = [], []
        for table in tables:
            try:
                table_body = table.find('tbody') # may be None
                headers.append(table_body.find_all('th'))
                rows.append(table_body.find_all('tr'))
            except:
                headers.append(table.findAll('th'))
                rows.append(table.findAll('tr')) 
        return headers, rows
        
       
    #/************************************************************************/
    @classmethod
    def load_url_table(cls, session, url, **kwargs): 
        status = cls.get_status(session, url)
        names = kwargs.pop('names')
        df=pd.read_table(url, encoding=str, skip_blank_lines=True, memory_map=True,
                             error_bad_lines=False, warn_bad_lines=True, **kwargs)
        return df
