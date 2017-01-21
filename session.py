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

import os 
import warnings
import time
   
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
    import datetime
except ImportError:          
    class datetime:
        class timedelta: 
            pass
    
try:
    import hashlib
except:
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

from . import EurobaseWarning, EurobaseError
from . import settings


#==============================================================================
# CLASSES/METHODS
#==============================================================================

class Session(object):
    """
    """
    
    def __init__(self, **kwargs):
        # initial default settings
        self.__session      = None
        self.__cache        = None
        self.__time_out     = 0 # datetime.deltatime(0)
        self.__force_download   = False
        # update with keyword arguments passed
        if kwargs != {}:
            attrs = ( 'time_out','force_download','cache' )
            for attr in list(set(attrs).intersection(kwargs.keys())):
                try:
                    setattr(self, '{}'.format(attr), kwargs.pop(attr))
                except: 
                    warnings.warn(EurobaseWarning('wrong attribute value {}'.format(attr.upper())))
        # initialise
        self.set(**kwargs)
        
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
    def time_out(self):
        return self.__time_out
    @time_out.setter
    def time_out(self, time_out):
        if not isinstance(time_out, (int, datetime.timedelta)):
            raise EurobaseError('wrong type for TIME_OUT parameter')
        elif isinstance(time_out, int) and time_out<0:
            raise EurobaseError('wrong setting for TIME_OUT parameter')
        self.__time_out = time_out

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
    def set(self, **kwargs):
        try:
            self.__session = requests.session(**kwargs)
        except:
            raise EurobaseError('wrong definition for SESSION parameter')
    def get(self, **kwargs):
        try:
            session = requests.session(**kwargs)
        except:
            session = None
            pass
        return session or self.__session
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
            filters = '&'.join(['{k}={v}'.format(k=k, v=v) for (k, v) in _izip_replicate(kwargs)])
            # filters = '&'.join(map("=".join,kwargs.items()))
            sep = '?'
            try:        
                last = url.rsplit('/',1)[1]
                if '?' in last:     sep = '&'
            except:     
                pass
            url = "{url}{sep}{filters}".format(url=url, sep=sep, filters=filters)
        return url
    
    #/************************************************************************/
    def get_status(self, url):
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
        print ('in get_response: {}'.format(self.__session))
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
                        
    #/************************************************************************/
    def load_page(self, url, **kwargs):
        """Download url from internet.
        Store the downloaded content into <cache>/file.
        If <cache>/file exists, it returns content from disk
        :param url: page to be downloaded
        :param filename: filename where to store the content of url, None if we want not store
        :param time_to_live: how many seconds to store file on disk,
            None use default time_to_live,
                                 0 don't use cached version if any
        :returns: the content of url (str type)
        """
        # create cache directory only the fist time it is needed
        # note: html must be a str type not byte type
        cache = kwargs.get('cache') or self.cache or None
        time_out = kwargs.get('time_out') or self.time_out or 0
        force_download = kwargs.get('force_download') or self.force_download or False
        pathname = self.__build_pathname(url, cache)
        if force_download or not self.__is_cached(pathname, time_out):
            response = self.get_response(url)
            html = response.text
            if cache is not None:
                if not os.path.exists(cache):
                    os.makedirs(cache)
                elif not os.path.isdir(cache):
                    raise EurobaseError('cache {} is not a directory'.format(cache))
                self.__write_to_cache(pathname, html)
        else:
            if not os.path.exists(cache) or not os.path.isdir(cache):
                raise EurobaseError('cache {} is not a directory'.format(cache))
            html = self.__read_from_cache(pathname)
        return pathname, html
    @staticmethod
    def __build_pathname(url, cache):
        pathname = url.encode('utf-8')
        try:
            pathname = hashlib.md5(pathname).hexdigest()
        except:
            pathname = pathname.hex()
        if cache is not None:
            pathname = os.path.join(cache, pathname)
        return pathname
    @staticmethod
    def __write_to_cache(pathname, content):
        """write content to pathname
        :param pathname:
        :param content:
        """
        with open(pathname, 'w') as f:
            f.write(content)
            f.close()  
        return
    @staticmethod
    def __read_from_cache(pathname):
        """it reads content from pathname
        :param pathname:
        """
        with open(pathname, 'r') as f:
            content = f.read()
            f.close()
        return content
    @staticmethod
    def __is_cached(pathname, time_out):
        if not os.path.exists(pathname):
            resp = False
        elif time_out is 0:
            resp = False
        elif time_out is None:
            resp = True
        else:
            cur = time.time()
            mtime = os.stat(pathname).st_mtime
            # print("last modified: %s" % time.ctime(mtime))
            resp = cur - mtime < time_out
        return resp

    #/************************************************************************/
    def is_cached(self, url):
        """check if url exists
        :param url:
        :returns: True if the file can be retrieved from the disk (cache)
        """
        pathname = self.__build_pathname(url, self.cache)
        return self.__is_cached(pathname, self.time_out)
        
    #/************************************************************************/
    @classmethod
    def read_soup_table(cls, html, **kwargs): # read vegetables
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
    def read_html_table(self, url, **kwargs): 
        try:
            self.get_status(url)
        except:
            return None
        # set some default values (some are already default values for read_table)
        kwargs.update({'encoding': kwargs.get('encoding') or None})
        # run pandas...
        df = pd.read_html(url, **kwargs)
        return df
               
    #/************************************************************************/
    def read_url_table(self, url, **kwargs): 
        try:
            self.get_status(url)
        except:
            return None
        # set some default values (some are already default values for read_table)
        kwargs.update({'encoding': kwargs.get('encoding') or None,
                        'skip_blank_lines': kwargs.get('skip_blank_lines') or True, 
                        'memory_map': kwargs.get('memory_map') or True,
                        'error_bad_lines': kwargs.get('error_bad_lines') or False, 
                        'warn_bad_lines': kwargs.get('warn_bad_lines') or True,
                        'compression': kwargs.get('compression') or 'infer'})
        # run pandas...
        df = pd.read_table(url, **kwargs)
        return df
