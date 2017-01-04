#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. eurobase.py

Tools for Eurostat data collections upload

**About**

*credits*:      `grazzja <jacopo.grazzini@ec.europa.eu>`_ 

*version*:      0.1
--
*since*:        Wed Dec  7 23:52:39 2016

**Description**

**Usage**

    >>> <put_here_an_example>
    
**Dependencies**

*call*:         <put_here_called_modules>

*require*:      <put_here_required_modules>
                :mod:`os`, :mod:`sys`, :mod:`string`, :mod:`inspect`, :mod:`warnings`, \ 
                :mod:`re`, :mod:`math`, :mod:`operator`, :mod:`itertools`, :mod:`collections`
                
*optional*:     <put_here_optional_modules>
                :mod:`numpy`, :mod:`scipy`, :mod:`matplotlib`, :mod:`pylab`,                        \
                :mod:`pickle`, :mod:`cPickle`

**Contents**
"""


#==============================================================================
# PROGRAM METADATA
#==============================================================================

metadata = dict([
                ('project', 'EuroBase'),
                ('date', 'Wed Dec  7 23:52:39 2016'),
                ('url', 'https://github.com/gjacopo/eurobase'),
                ('organization', 'European Union'),
                ('license', 'European Union Public Licence (EUPL)'),
                ('version', '0.1'),
                ('description', 'Tools for data collections upload from Eurostat website'),
                ('credits',  ['grazzja']),
                ('contact', 'jacopo.grazzini@ec.europa.eu'),
                ])


#==============================================================================
# IMPORT STATEMENTS
#==============================================================================

import os, sys 
import inspect, warnings
import string, re, time
import operator, itertools, collections

    
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
    raise IOError
    
try:
    import hashlib
except:
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

try:                                
    import datetime
except ImportError:          
    class datetime:
        class timedelta: 
            pass


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

class EurobaseError(Exception):
    """Base class for exceptions in this module."""
    def __init__(self, msg, expr=None):    
        self.msg = msg
        if expr is not None:    self.expr = expr
    def __str__(self):              return repr(self.msg)
class EurobaseWarning(Warning):
    """Base class for warnings in this module."""
    def __init__(self, msg, expr=None):    
        self.msg = msg
        if expr is not None:    self.expr = expr
    def __str__(self):              return repr(self.msg)


PROTOCOLS       = ('http', 'https', 'ftp')
DEF_PROTOCOL    = 'http'
LANGS           = ('en','de','fr')
DEF_LANG        = 'en'
FMTS            = ('json','unicode')
DEF_FMT        = 'json'

BULK_DOMAIN     = 'ec.europa.eu/eurostat/estat-navtree-portlet-prod/'
BULK_QUERY      = 'BulkDownloadListing'
BULK_DIC_FILE   = 'dic'
BULK_DIC_LIST   = 'dimlist'
BULK_DIC_EXT    = 'dic'
BULK_DATA_DIR   = 'data'
BULK_DATA_EXT   = 'tsv.gz'
BULK_META_FILE  = 'metabase'
BULK_META_EXT   = 'txt.gz'

API_DOMAIN      = 'ec.europa.eu/eurostat/wdds/rest/data'
API_VERS        = 2.1
API_PRECISION   = 1 # only available at the moment? 

KW_DEFAULT      = 'default'

BS_PARSERS      = ("html.parser","html5lib","lxml","xml")

#==============================================================================
# CLASSES/METHODS
#==============================================================================

class Request(object):
    """
    """
    
    
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
        else:                       protocol = DEF_PROTOCOL
        if protocol not in PROTOCOLS:
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
    @classmethod
    def get_status(cls, session, url):
        """Download just the header of a URL and return the server's status code.
        
            >>> status = Requests.get_status(session, url)
            
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
             response = session.head(url)
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
    @classmethod
    def get_response(cls, session, url):
        """
        
            >>> response = Requests.get_response(session, url)
            
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
             response = session.get(url) # session.request('get',url)
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
    @classmethod
    def __decode_error(cls, error):
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


class Collections(object):
    """
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=dic/en/net_seg10.dic    
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=dic/en/dimlist.dic    

    dimensions:
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=dic/en/net_seg10.dic    
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=dic/en/dimlist.dic    
 
    datasets:
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?dir=data&sort=1&start=a
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=metabase.txt.gz
    """
    
    #/************************************************************************/
    def __init__(self, **kwargs):
        """
        :param cache: directory where to store downloaded files
        :param expire: how many seconds to store file on disk, None is infinity, 0 for not to store
        """
        self.__session      = None
        self.__url          = None
        self.__status       = None
        self.__metabase     = None
        # set default values
        self.__domain       = BULK_DOMAIN
        self.__lang         = DEF_LANG
        self.__query        = BULK_QUERY
        self.__dimensions   = []
        self.__datasets     = dict([(a, []) for a in list(string.ascii_lowercase)])
        self.__cache        = None
        self.__expire       = 0 # datetime.deltatime(0)
        self.__force_download   = False
        # check whether any argument is passed
        if kwargs == {}:
            return
        # update
        attrs = ( 'domain','query','lang','expire','force_download',     \
                              'dimensions','datasets','cache' )
        for attr in list(set(attrs).intersection(kwargs.keys())):
            try:
                setattr(self, '{}'.format(attr), kwargs.pop(attr))
            except: 
                warnings.warn(EurobaseWarning('wrong attribute value {}'.format(attr.upper())))
        
    #/************************************************************************/
    @property
    def domain(self):
        return self.__domain
    @domain.setter
    def domain(self, domain):
        if not isinstance(domain, str):
            raise EurobaseError('wrong type for DOMAIN parameter')
        self.__domain = domain

    #/************************************************************************/
    @property
    def query(self):
        return self.__query
    @query.setter
    def query(self, query):
        if not isinstance(query, str):
            raise EurobaseError('wrong type for QUERY parameter')
        self.__query = query

    #/************************************************************************/
    @property
    def lang(self):
        return self.__lang
    @lang.setter
    def lang(self, lang):
        if not isinstance(lang, str):
            raise EurobaseError('wrong type for LANG parameter')
        elif not lang in LANGS:
            raise EurobaseError('language not supported')
        self.__lang = lang

    #/************************************************************************/
    @property
    def dimensions(self):
        return self.__dimensions
    @dimensions.setter
    def dimensions(self, dimensions):
        if not isinstance(dimensions, (str,list,tuple)):
            raise EurobaseError('wrong type for DIMENSIONS parameter')
        elif isinstance(dimensions, str):
            dimensions = [dimensions]
        self.__dimensions = dimensions

    #/************************************************************************/
    @property
    def datasets(self):
        return [items for lists in self.__datasets.values() for items in lists]
    @datasets.setter
    def datasets(self, datasets):
        if not isinstance(datasets, (dict, list, tuple)):
            raise EurobaseError('wrong type for DATASETS parameter')
        elif isinstance(datasets, (list, tuple)):
            datasets = {'_all_': datasets}
        self.__datasets = datasets # not an update!

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
    def setURL(self, **kwargs):
        """Set the query URL to *Bulk download* web service.
        
            >>> url = Collections._build_url(domain, **kwargs)
           
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
    
        Example: 
            http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?dir=comp
            http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&dir=dic

        # DIC_URL         = 'ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&dir=dic'
        # DATA_URL        = 'ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&dir=data'
    
        """
        [kwargs.update({attr: kwargs.get(attr) or getattr(self, '{}'.format(attr))})
            for attr in ('domain','query','lang')]
        self.__url=self.__build_url(**kwargs)
    def getURL(self, **kwargs):
        # update/merge passed arguments with already existing ones
        [kwargs.update({attr: kwargs.get(attr) or getattr(self, '{}'.format(attr))})
            for attr in ('domain','query','lang')]
        # actually return
        return self.__build_url(**kwargs) or self.__url
    @property
    def url(self):
        #if self._url is None:   self.setURL()
        return self.__url
    @staticmethod
    def __build_url(**kwargs):
        if kwargs == {}:
            return None
        elif not('domain' in kwargs):
            raise EurobaseError('uncomplete information for building URL')
        # set parameters
        domain = kwargs.pop('domain')
        if 'lang' in kwargs:  
            lang = kwargs.pop('lang')
        else:
            lang = None
        if lang not in LANGS:
            raise EurobaseError('language not supported')
        if 'sort' in kwargs:    sort = kwargs.pop('sort')
        else:                   sort = 1    
        if not isinstance(sort,int):
            raise EurobaseError('wrong parameter value for sort')            
        kwargs.update({'sort':sort}) 
        url = Request.build_url(domain, **kwargs)
        if lang is not None:
            url = "{url}/{lang}".format(url=url,lang=lang)
        return url
        
    #/************************************************************************/
    def setSession(self, **kwargs):
        self.__session = requests.session(**kwargs)
    def getSession(self, **kwargs):
        return requests.session(**kwargs) or self.__session
    @property
    def session(self):
        return self.__session

    #/************************************************************************/
    def is_cached(self, url):
        """check if url exists
        :param url:
        :returns: True if the file can be retrieved from the disk (cache)
        """
        pathname = self.__build_pathname(url, dirname=self.__cache)
        return self.__is_cached(pathname, expire=self.__expire)
    @staticmethod
    def __is_cached(pathname, expire=0):
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
    @staticmethod
    def __build_pathname(filename, dirname=None):
        pathname = filename.encode('utf-8')
        try:
            pathname = hashlib.md5(pathname).hexdigest()
        except:
            pathname = pathname.hex()
        if dirname is not None and dirname != '':
            pathname = os.path.join(dirname, pathname)
        return pathname
        
    #/************************************************************************/
    def get_members(self, url, **kwargs):
        pathname = self.__build_pathname(url, dirname=self.__cache)
        # update/merge passed arguments with already existing ones
        [kwargs.update({attr: kwargs.get(attr) or getattr(self, '{}'.format(attr))})
            for attr in ('force_download','expire')]
        # actually return
        return self.__get_members(pathname, **kwargs)
    @staticmethod
    def __get_members(url, pathname, **kwargs):
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
        force_download = kwargs.get('force_download',False)
        expire = kwargs.get('expire', 0)
        # create cache directory only the fist time it is needed
        if not os.path.exists(self.__cache):
            os.makedirs(self.__cache)
        if not os.path.isdir(self.__cache):
            raise EurobaseError('cache {} is not a directory'.format(self.__cache))
        # note: html must be a str type not byte type
        if force_download is True or expire == 0 or not self.__is_cached(pathname):
                response = cls.get_response(self.__session, url)
                html = response.text
                self.__write_to_cache(pathname, html)
            else:
                html = self.__read_from_cache(pathname)
            return html

    #/************************************************************************/
    @staticmethod
    def __read_members(html):
        if html is None or html == '':
            return None
        headers, rows = Request.read_html_table(html, attrs={'class':'filelist'})
        #_, rows = Requests.fetch_tables(soup,class_='filelist')
        rows = rows[0] # only one table in the page
        data, i = [], 0
        for row in rows:
            i = i+1
            try:
                cols = row.find_all("td")
            except:
                cols = row.findAll("td")
            if cols == [] or i == 1:  
                continue
            else:
                data.append(cols[0].find('a').find(text=True))
            return data    
            
    #/************************************************************************/
    def find_dimensions(self, **kwargs):
        if kwargs == {}:    url = self.url
        else:               url = self.get_url(**kwargs)
        #kwargs.update({'file': self.BULK_FILE})
        #url = self.getURL(**kwargs)
        url = '{url}&file={fil}'.format(url=url, fil=BULK_DIC_FILE)
        html = self.__get_members(url)
        data = self.__read_members(html)
        dimensions = [d.replace('.{ext}'.format(ext=BULK_DIC_EXT),'') for d in data]
        return dimensions
     
    #/************************************************************************/
    def find_datasets(self, alpha=None, **kwargs):
        if alpha is None:
            alpha = list(string.ascii_lowercase)
        elif not alpha in list(string.ascii_lowercase):
            raise EurobaseError('unrecognised parameter alpha')
        else:
            alpha = list(alpha)
        datasets = {} # all_datasets = []
        if kwargs == {}:    url = self.url
        else:               url = self.get_url(**kwargs)
        for a in alpha:
            urla = '{url}&dir={dire}&start={alpha}'.format(url=url, dire=BULK_DATA_DIR, alpha=a)
            html = self.__get_members(urla)
            data = self.__read_members(html)
            datasets[a] = [d.replace('.{ext}'.format(ext=BULK_DATA_EXT),'') for d in data]
            #all_datasets.append(datasets[a])
        return datasets #all_datasets
            
    @property
    def members(self):
        #if self._members is None:   self.setMembers()
        if isinstance(self.__members, (list,tuple)):
            return self.__members
        elif isinstance(self.__members, dict):
            return [items for lists in self.__members.values() for items in lists]        
    @members.setter
    def members(self, members):
        if not isinstance(members, type(self.__members)):
            raise EurobaseError('wrong type for members')
        self.__members = members
 
    #/************************************************************************/
    @staticmethod
    def __check_member(member, members):
        if member in members:      
            return True
        else:                       
            return False
    def check_dimensions(self, dimension):
        return self.__check_member(dimension, self.dimensions)
    def check_datasets(self, dataset):
        return self.__check_member(dataset, self.datasets)
    
    @staticmethod
    def __write_to_cache(pathname, content):
        """write content to pathname
        :param pathname:
        :param content:
        """
        f = open(pathname, 'w')
        f.write(content)
        f.close()
    
    @staticmethod
    def __read_from_cache(pathname):
        """it reads content from pathname
        :param pathname:
        """
        f = open(pathname, 'r')
        content = f.read()
        f.close()
        return content

            #/************************************************************************/
    def getDimensions(self, dataset, **kwargs):
        if self._metabase is None:
                self.setMetabase(**kwargs)
        return self.__metabase
    
    #/************************************************************************/
    def getMetabase(self, dataset, **kwargs):
        if self.url is None:
            self.setURL(**kwargs)
        #kwargs.update({'dir': self.BULK_DIR, 'start': a})
        #url = self.getURL(**kwargs)
        url = '{url}&fil={fil}{ext}'.format(url=self.url, fil=self.METAFILE, ext=self.META_FMT)
        kwargs.update({'names': ['dataset', 'dimension', 'label'],  compression: 'gzip'})
        return Request.read_url_table(url, **kwargs)
      
    
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
                warnings.warn(EurobaseWarning('wrong attribute value {}'.format(attr.upper())))

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
    @staticmethod
    def _check_format(fmt):
        if not(fmt is None or fmt in API.REST_FMTS):        
            raise EurobaseError('unrecognised format')
        else:                           
            return fmt
    def setFmt(self, fmt):
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
        """
        self._fmt=self._check_format(fmt)       
    def getFmt(self, fmt=None):
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
        """
        return self._check_format(fmt) or self._fmt
    @property
    def fmt(self):
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
        """
        return self._fmt      
    @fmt.setter
    def fmt(self,fmt):
        self.setFmt(fmt)
           
    #/************************************************************************/
    @staticmethod
    def _check_language(language):
        if not(language is None or language in API.REST_LANGS):     
            raise EurobaseError('unrecognised language') 
        else:                               
            return language
    def setLang(self, language):
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
        """
        self._lang=self._check_language(language)       
    def getLang(self, language=None):
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
        """
        return self._check_language(language) or self._lang
    @property
    def lang(self):
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
        """
        return self._lang      
    @lang.setter
    def lang(self,lang):
        self.setLang(lang)
      
    #/************************************************************************/
    @staticmethod
    def _check_version(version):
        if not(version is None or isinstance(version,float)): 
            raise EurobaseError('wrong format/unrecognised version')
        elif version < API.VERS_REST:           
            raise EurobaseError('version not supported') 
        else:                               
            return version
    def setVers(self, version):
        self._vers=self._check_version(version)       
    def getVers(self, version=None):
        return self._check_version(version) or self._vers
    @property
    def vers(self):
        return self._vers     
    @vers.setter
    def vers(self,vers):
        self.setVers(vers)
      
    #/************************************************************************/
    @staticmethod
    def _check_domain(domain):
        if not(domain is None or isinstance(domain,str)):    
            raise EurobaseError('wrong format/unrecognised domain') 
        else:                               
            return domain
    def setDomain(self, domain):
        self._domain=self._check_domain(domain)       
    def getDomain(self, domain=None):
        return self._check_domain(domain) or self._domain
    @property
    def domain(self):
        return self._domain     
    @domain.setter
    def domain(self,domain):
        self.setDomain(domain)
      
    #/************************************************************************/
    @staticmethod
    def _check_dataset(dataset):
        if not(dataset is None or isinstance(dataset,str)):   
            raise EurobaseError('wrong format/unrecognised dataset') 
        # CHECK DIMENSIONS!!!
        # elif dataset < VERS_REST:            raise EurobaseError('version not supported')
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
            raise EurobaseError('wrong format/unrecognised precision')
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
        if status is None:                      raise EurobaseError('unknown status')
        elif not isinstance(status, int):       raise EurobaseError('unrecognised status')
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
            raise EurobaseError('uncomplete information for building URL')
        # set parameters
        vers=kwargs.pop('vers') 
        fmt=kwargs.pop('fmt') 
        lang=kwargs.pop('lang')
        kwargs.update({'path': "v{vers}/{fmt}/{lang}".format(vers=vers,fmt=fmt,lang=lang)}) 
        if 'precision' not in kwargs:   
            kwargs.update({'precision': 1})
        url = Request.build_url(**kwargs)
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
        if kwargs.get(KW_DEFAULT) is True:  
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
            raise EurobaseError('Request URL not (re)set')
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
        """
        if url is None or url=='':
            url=self.url
        self.status, response = Request.get_data(url, fmt=self.fmt)
        try:
            resp = response.read()
        except:
            raise EurobaseError('error reading URL')    
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
                    raise EurobaseError('method/attribute {} not implemented'.format(attr))
      
      
      
#==============================================================================
# MAIN METHOD AND TESTING AREA
#==============================================================================
def main():
    """eurobase.py main()"""
    return
    
if __name__ == '__main__':
    main()
    
