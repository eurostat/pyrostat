# -*- coding: utf-8 -*-

"""
.. collections.py

Basic class for dimensions/datasets definitions

**About**

*credits*:      `grazzja <jacopo.grazzini@ec.europa.eu>`_ 

*version*:      0.1
--
*since*:        Wed Jan  4 01:57:24 2017

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

**Contents**
"""



from .settings import *
from .requests import Requests

#==============================================================================
# CLASSES/METHODS
#==============================================================================
    

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
        url = Requests.build_url(domain, **kwargs)
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
      
