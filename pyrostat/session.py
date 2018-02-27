#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. session.py

Basic class for common request operations 

**About**

*credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_ 

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
import StringIO
import warnings
import time

import inspect
   
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
    class pd:
        def read_table(*args, **kwargs): 
            raise IOError
        def read_html(*args, **kwargs): 
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

from . import pyroWarning, pyroError
from . import settings


#==============================================================================
# CLASSES/METHODS
#==============================================================================

class Session(object):
    """
    """
    
    def __init__(self, **kwargs):
        # initial default settings
        self._session           = None
        self._cache             = True
        self._cache_backend     = None
        self._force_download    = False
        self._expire_after      = None # datetime.deltatime(0)
        # update with keyword arguments passed
        if kwargs != {}:
            attrs = ( '_expire_after','force_download','cache')
            for attr in list(set(attrs).intersection(kwargs.keys())):
                try:
                    setattr(self, '{}'.format(attr), kwargs.get(attr))
                except: 
                    warnings.warn(pyroWarning('wrong attribute value {}'.format(attr.upper())))
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
        return self._cache
    @cache.setter
    def cache(self, cache):
        if isinstance(cache, str):
            self._cache = os.path.abspath(cache)
        elif isinstance(cache, bool):
            self._cache = cache
        else:
            raise pyroError('wrong type for CACHE_NAME parameter')
        

    #/************************************************************************/
    @property
    def cache_backend(self):
        return self._cache_backend
    @cache_backend.setter
    def cache_backend(self, backend):
        if backend is None:
            self._cache_backend = 'sqlite' # None
        elif backend in ('sqlite', 'memory', 'dict', 'file', 'redis', 'mongo'):
            self._cache_backend = backend
        elif not isinstance(backend, str):
            raise pyroError('wrong type for CACHE_BACKEND parameter')
        elif backend is not None:
            raise pyroError('wrong backend setting for CACHE_BACKEND parameter')

    #/************************************************************************/
    @property
    def expire_after(self):
        return self._expire_after
    @expire_after.setter
    def expire_after(self, expire_after):
        if expire_after is None or isinstance(expire_after, (int, datetime.timedelta)) and int(expire_after)>=0:
            self._expire_after = expire_after
        elif not isinstance(expire_after, (int, datetime.timedelta)):
            raise pyroError('wrong type for EXPIRE_AFTER parameter')
        elif isinstance(expire_after, int) and expire_after<0:
            raise pyroError('wrong time setting for EXPIRE_AFTER parameter')

    #/************************************************************************/
    @property
    def force_download(self):
        return self._force_download
    @force_download.setter
    def force_download(self, force_download):
        if not isinstance(force_download, bool):
            raise pyroError('wrong type for FORCE_DOWNLOAD parameter')
        self._force_download = force_download
        
    #/************************************************************************/
    def get(self, **kwargs):
        cache = kwargs.pop('cache', None)
        if cache is None or (cache is False and self.cache in (None,False)): 
\            try:
                session = requests.session(**kwargs)
            except:
                session = None
                pass
        else:
            if cache is None and not self.cache in (None,False):
                cache = self.cache   
            if cache is True:
                cache = self.__default_cache()
            try:
                kwargs.update({'expire_after': kwargs.get('expire_after') or self.expire_after,
                                   'backend': kwargs.pop('cache_backend',None) or self.cache_backend})
                session = requests_cache.CachedSession(cache_name=cache, **kwargs)
            except:
                session = None
                pass
        return session
    def set(self, **kwargs):
        try:
            self._session = self.get(**kwargs)
            assert self._session is not None
        except:
            raise pyroError('wrong definition for SESSION parameter')
    @property
    def session(self):
        return self._session
        
    #/************************************************************************/
    @classmethod
    def build_url(cls, *args, **kwargs):
        """Create a query URL to *Eurobase* web service.
        
            >>> url = Requests.build_url(*args, **kwargs)
           
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
        if args not in (None,()):       domain = args[0]
        else:                           domain = kwargs.pop('domain','')
        url = domain.strip("/")
        protocol = kwargs.pop('protocol', settings.DEF_PROTOCOL)
        if protocol not in settings.PROTOCOLS:
            raise pyroError('web protocol not recognised')
        if not url.startswith(protocol):  
            url = "%s://%s" % (protocol, url)
        if 'path' in kwargs:      
            url = "%s/%s" % (url, kwargs.pop('path'))
        if 'query' in kwargs:      
            url = "%s/%s?" % (url, kwargs.pop('query'))
        if kwargs != {}:
            #_izip_replicate = lambda d : [(k,i) if isinstance(d[k], (tuple,list))        \
            #        else (k, d[k]) for k in d for i in d[k]]
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
            try:        
                last = url.rsplit('/',1)[1]
                if any([last.endswith(c) for c in ('?', '/')]):     sep = ''
            except:     
                sep = '?'
            url = "%s%s%s" % (url, sep, filters)
        return url
    
    #/************************************************************************/
    def get_status(self, url):
        """Download just the header of a URL and return the server's status code.
        
            >>> session = Session()
            >>> status = session.get_status(url)
            
        Arguments
        ---------
        url : str

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
             response = self._session.head(url)
             response.raise_for_status()
        except:
             raise pyroError('wrong request formulated')  
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
        #     raise ESDataError('Error {} : {}'.format(status, msg))  
        # else:
        #     status = response.getcode()
        #     response.close()
        return status # in requests.codes.ok ?
          
    #/************************************************************************/
    def get_response(self, url, **kwargs):
        """
        
            >>> session = Session()
            >>> response = session.get_response(url)
            
        Arguments
        ---------
        url : str
            
        Returns
        -------

        Raises
        ------
        ESDataError

        Note
        ----
                
        Examples
        --------
        >>> 
        
        See also
        --------
        """
        cache = kwargs.pop('cache',None)
        force_download = kwargs.pop('force_download', False)
        try:
            if cache is None or (force_download is True and self.cache in (None,False)):
                response = self._session.get(url)                
            elif (cache is False and self.cache not in (None,False)) or force_download is True:
                with requests_cache.disabled():
                    response = self._session.get(url)                
            elif self.cache in (None,False):
                if isinstance(cache, bool) and cache is True:
                    cache = self.__default_cache()
                with requests_cache.enabled(cache, **kwargs):
                    response = self._session.get(url)                
        except:
            raise pyroError('wrong request formulated')  
        else:
            response.raise_for_status()
        ## urllib2 variant
        # request = urllib2.Request(url)
        # try:
        #     response = urllib2.urlopen(request)
        # except urllib2.HTTPError as e:
        #     status, msg = cls.__decode_error(e)
        #     raise ESDataError('Error {} : {}'.format(status, msg))  
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
    def load_page(self, url, **kwargs):
        """Download url from internet and store the downloaded content into <cache>/file.
        If <cache>/file already exists, it returns content from disk
        
            >>> page = S.load_page(url, cache_download=False, time_out=0)
        
        Argunent
        --------
        url : str
            basic URL path of the page to be downloaded.

        Keyword Arguments
        ----------------- 
        force_download : bool
            flag set to force the download even if the file already exists in the 
            cache; default: `False`
        cache_store : str/bool
            directory where to store (cache) the content of URL; when set to `True`, 
            a default cache directory is defined; default: `False` and not cache is
            used for storage
        time_out : int
            how many seconds to store file on disk; if `None`, use default , if 0,
            don't use cached version if any.
            
        Returns
        -------
        page : str
            the content of the URL `url`.
        """
        # create cache directory only the fist time it is needed
        # note: html must be a str type not byte type
        cache_store = kwargs.get('cache_store') or self.cache_store or False
        force_download = kwargs.get('force_download') or self.force_download or False
        time_out = kwargs.get('time_out') or self.time_out or 0
        if isinstance(cache_store, bool) and cache_store is True:
            cache_store = self.__default_cache()
        pathname = self.__build_pathname(url, cache_store)
        if force_download is True or not self.__is_cached(pathname, time_out):
            response = self.get_response(url)
            html = response.text
            if cache_store is not None:
                if not os.path.exists(cache_store):
                    os.makedirs(cache_store)
                elif not os.path.isdir(cache_store):
                    raise pyroError('cache {} is not a directory'.format(cache_store))
                self.__write_to_pathname(pathname, html)
        else:
            if not os.path.exists(cache_store) or not os.path.isdir(cache_store):
                raise pyroError('cache {} is not a directory'.format(cache_store))
            html = self.__read_from_pathname(pathname)
        return pathname, html
    @staticmethod
    def __build_pathname(url, cache):
        """Build unique filename from URL name and cache directory, _e.g._ using 
        hashlib encoding.
        """
        pathname = url.encode('utf-8')
        try:
            pathname = hashlib.md5(pathname).hexdigest()
        except:
            pathname = pathname.hex()
        if cache not in (False,''):
            pathname = os.path.join(cache, pathname)
        return pathname
    @staticmethod
    def __write_to_pathname(path, content):
        """Write "content" to a given pathname.
        """
        with open(path, 'w') as f:
            f.write(content)
            f.close()  
        return
    @staticmethod
    def __dump_to_pathname(path, content, protocol=2):
        """Dump "content" to a given pathname.
        """
        with open(path, 'wb') as f:
            pickle.dump(content, f, protocol=protocol)
            f.close()  
        return
    @staticmethod
    def __read_from_pathname(path):
        """Read "content" from a given pathname.
        """
        with open(path, 'r') as f:
            content = f.read()
            f.close()
        return content
    @staticmethod
    def __load_from_pathname(path):
        """Load "content" from a given pathname.
        """
        with open(path, 'rb') as f:
            try:
                content = pickle.load(f, encoding="ascii", errors="replace")
            except TypeError:
                content = pickle.load(f)
            f.close()
        return content
       
    #/************************************************************************/
    @staticmethod
    def __fileexists(file):
        """Check file existence.
        """
        return os.path.exists(os.path.abspath(file))

    #/************************************************************************/
    @staticmethod
    def __default_cache():
        """Create default pathname for cache directory depending on OS platform.
        Inspired by `Python` package `mod:wbdata`: default path defined for 
        `property:path` property of `class:Cache` class.
        """
        platform = sys.platform
        if platform.startswith("win"): # windows
            basedir = os.getenv("LOCALAPPDATA",os.getenv("APPDATA",os.path.expanduser("~")))
        elif platform.startswith("darwin"): # Mac OS
            basedir = os.path.expanduser("~/Library/Caches")
        else:
            basedir = os.getenv("XDG_CACHE_HOME",os.path.expanduser("~/.cache"))
        return os.path.join(basedir, settings.PACKAGE)    
    @staticmethod
    def __is_cached(pathname, time_out):
        """Check whether a URL exists and is alread cached.
        :param url:
        :returns: True if the file can be retrieved from the disk (cache)
        """
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
    def is_cached(self, url):
        """Check whether a URL exists and is alread cached.
        :param url:
        :returns: True if the file can be retrieved from the disk (cache)
        """
        return self.__is_cached(self.__build_pathname(url, self.cache), self.time_out)
        
    #/************************************************************************/
    @classmethod
    def read_soup_table(cls, html, **kwargs): # read vegetables
        """
        
            >>> read_soup_table(cls, html, **kwargs)
            
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
            raise pyroError('unknown soup parser')
        ## urllib2 variant
        # html = response.read()
        try:
            raw = bs4.BeautifulSoup(html, parser)
            #raw = bs4.BeautifulSoup(html, parser).get_text()
        except:
            raise pyroError('impossible to read HTML page') 
        try:
            tables = raw.findAll('table', **kwargs)
        except:
            raise pyroError('error with soup from HTML page')
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
        try:
            req = self.get(url)
            content = StringIO.StringIO(req.content)
        except:
            raise pyroError('wrong request')
        # set some default values (some are already default values for read_table)
        kwargs.update({'encoding': kwargs.get('encoding') or None})
        [kwargs.pop(key) for key in kwargs                                          \
             if key not in list(inspect.signature(pandas.read_table).parameters.keys())]
        # run pandas...
        return pd.read_html(content, **kwargs)
               
    #/************************************************************************/
    def read_url_table(self, url, **kwargs) ->pd.Dataframe: 
        try:
            self.get_status(url)
        except:
            return None
        try:
            req = self.get(url, **kwargs)
            content = StringIO.StringIO(req.content)
        except:
            raise pyroError('wrong request')
        # set some default values (some are already default values for read_table)
        kwargs.update({'encoding': kwargs.get('encoding') or None,
                        'skip_blank_lines': kwargs.get('skip_blank_lines') or True, 
                        'memory_map': kwargs.get('memory_map') or True,
                        'error_bad_lines': kwargs.get('error_bad_lines') or False, 
                        'warn_bad_lines': kwargs.get('warn_bad_lines') or True,
                        'compression': kwargs.get('compression') or 'infer'})
        parameters = inspect.signature(pandas.read_table).parameters
        keys = [key for key in kwargs.keys()                                          \
             if key not in list(parameters.keys()) or parameters[key].KEYWORD_ONLY.value==0]
        [kwargs.pop(key) for key in keys]
        # run pandas...
        df = pd.read_table(content, **kwargs)
        return df

