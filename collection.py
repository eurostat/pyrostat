# -*- coding: utf-8 -*-

"""
.. database.py

Basic class for online database (dimensions+datasets) definitions

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


#==============================================================================
# IMPORT STATEMENTS
#==============================================================================

import warnings
import string

try:                                
    import lxml
except ImportError:                 
    pass

from . import EurobaseWarning, EurobaseError
from . import settings
from .session import Session

#==============================================================================
# CLASSES/METHODS
#==============================================================================
    

class Collection(object):
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
        self.__domain       = settings.BULK_DOMAIN
        self.__lang         = settings.DEF_LANG
        self.__sort         = settings.DEF_SORT
        self.__query        = settings.BULK_QUERY
        self.__dimensions   = []
        self.__datasets     = dict([(a, []) for a in list(string.ascii_lowercase)])
        # check whether any argument is passed
        if kwargs == {}:
            return
        # update
        attrs = ( 'domain','query','lang','sort','dimensions','datasets' )
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
        elif not lang in settings.LANGS:
            raise EurobaseError('language not supported')
        self.__lang = lang

    #/************************************************************************/
    @property
    def sort(self):
        return self.__sort
    @sort.setter
    def sort(self, sort):
        if not isinstance(sort, int):
            raise EurobaseError('wrong type for SORT parameter')
        elif not sort > 0:
            raise EurobaseError('wrong value for SORT parameter')
        self.__sort = sort

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
    def setSession(self, **kwargs):
        try:
            self.__session = Session(**kwargs)
        except:
            raise EurobaseError('wrong definition for SESSION parameter')
    def getSession(self, **kwargs):
        try:
            session = Session(**kwargs)
        except:
            session = None
            pass
        return session or self.__session
    @property
    def session(self):
        return self.__session #.session

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
        if 'lang' in  kwargs:   kwargs.pop('lang')
        [kwargs.update({attr: kwargs.get(attr) or getattr(self, '{}'.format(attr))})
            for attr in ('query','sort')]
        self.__url=Session.build_url(self.domain, **kwargs)
    def getURL(self, **kwargs):
        # update/merge passed arguments with already existing ones
        if 'lang' in  kwargs:   kwargs.pop('lang')
        [kwargs.update({attr: kwargs.get(attr) or getattr(self, '{}'.format(attr))})
            for attr in set(('query','sort')).intersection(set(kwargs.keys()))]
        # actually return
        return Session.build_url(self.domain, **kwargs) or self.url
    @property
    def url(self):
        #if self._url is None:   self.setURL()
        return self.__url
            
    #/************************************************************************/
    def bulk_dimensions(self, **kwargs):
        if 'ext' in kwargs:     ext = kwargs.pop('ext')
        else:                   ext = settings.BULK_DIC_EXT
        if 'dir' in kwargs:     dire = kwargs.pop('dir')
        else:                   dire = settings.BULK_DIC_FILE
        url = self.__complete_url(self.url, lang=self.lang, dir=dire)
        html = self.session.load_page(url, **kwargs)
        if html is None or html == '':
            raise EurobaseError('no HTML content found') 
        _, rows = self.session.read_html_table(html, attrs={'class':'filelist'})
        data = self.__filter_table(rows)
        dimensions = [d.replace('.{ext}'.format(ext=ext),'') for d in data]
        return dimensions
    def bulk_datasets(self, alpha=None, **kwargs):
        if alpha is None:
            alpha = list(string.ascii_lowercase)
        elif not alpha in list(string.ascii_lowercase):
            raise EurobaseError('unrecognised parameter alpha')
        else:
            alpha = list(alpha)
        datasets = {} # all_datasets = []
        if 'ext' in kwargs:     ext = kwargs.pop('ext')
        else:                   ext = settings.BULK_DATA_EXT
        if 'dir' in kwargs:     dire = kwargs.pop('dir')
        else:                   dire = settings.BULK_DATA_DIR
        #url = '{url}&dir={dire}'.format(url=url, dire=dire)
        url = self.__complete_url(self.url, dir=dire)
        for a in alpha:
            urla = '{url}&start={alpha}'.format(url=url, alpha=a)
            html = self.session.load_page(urla, **kwargs)
            if html is None or html == '':
                raise EurobaseError('no HTML content found') 
            _, rows = self.session.read_html_table(html, attrs={'class':'filelist'})
            data = self.__filter_table(rows)
            datasets[a] = [d.replace('.{}'.format(ext),'') for d in data]
            #all_datasets.append(datasets[a])
        return datasets #all_datasets
    @staticmethod
    def __complete_url(domain, **kwargs):
        if kwargs == {}:
            return domain
        # set parameters
        if 'lang' in kwargs:    lang = kwargs.pop('lang')
        else:                   lang = None
        if lang is not None and lang not in settings.LANGS:
            raise EurobaseError('language not supported')
        url = Session.build_url(domain, **kwargs)
        if lang is not None:
            url = "{url}/{lang}".format(url=url,lang=lang)
        return url
    @staticmethod
    def __filter_table(rows):
        rows = rows[0] # only one table in the page
        data, i = [], 0
        for row in rows:
            i = i+1
            try:
                cols = row.find_all("td")
            except:
                cols = row.findAll("td")
            if cols == [] or i <= 2:  
                continue
            else:
                data.append(cols[0].find('a').find(text=True))
        return data    
 
    #/************************************************************************/
    @staticmethod
    def __check_member(member, members):
        if members is None or members==[]:
            raise EurobaseError('no members to compare to')
        if member in members:      
            return True
        else:                       
            return False
    def check_dimension(self, dimension):
        return self.__check_member(dimension, self.dimensions)
    def check_dataset(self, dataset):
        return self.__check_member(dataset, self.datasets)

    #/************************************************************************/
    def getDimensions(self, dataset, **kwargs):
        if self._metabase is None:
                self.setMetabase(**kwargs)
        return self.__metabase
    
    #/************************************************************************/
    def getMetabase(self, **kwargs):
        if 'ext' in kwargs:     ext = kwargs.pop('ext')
        else:                   ext = settings.BULK_META_EXT
        if 'file' in kwargs:    fil = kwargs.pop('file')
        else:                   fil = settings.BULK_META_FILE
        #if kwargs == {}:        
        url = self.url
        #else:                   url = self.get_url(**kwargs)
        url = '{url}&fil={fil}{ext}'.format(url=url, fil=fil, ext=ext)
        kwargs.update({'header': kwargs.get('header') or None, # no effect...
                       'names': kwargs.get('names') or ['dataset', 'dimension', 'label']})
        return self.session.load_file_table(url, **kwargs)
      
