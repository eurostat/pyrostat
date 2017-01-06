# -*- coding: utf-8 -*-

"""
.. bulkdata.py

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
from itertools import zip_longest
from collections import OrderedDict

import numpy as np

try:
    import pandas as pd
except ImportError:          
    pass

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
    

class Bulkdata(object):
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
        self.__dimensions   = {}
        self.__datasets     = {} # dict([(a, []) for a in list(string.ascii_lowercase)])
        # update
        if kwargs != {}:
            attrs = ( 'domain','query','lang','sort','dimensions','datasets' )
            for attr in list(set(attrs).intersection(kwargs.keys())):
                try:
                    setattr(self, '{}'.format(attr), kwargs.pop(attr))
                except: 
                    warnings.warn(EurobaseWarning('wrong attribute value {}'.format(attr.upper())))
        self.setSession(**kwargs)   # accepts keywords: time_out, force_download, cache
        self.setURL(**kwargs)       # accepts keywords: query, sort
       
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
    def metabase(self):
        return self.__metabase
    @metabase.setter
    def metabase(self, metabase):
        if isinstance(metabase, np.array):
            metabase = pd.DataFrame(data=metabase)
        elif not isinstance(metabase, pd.DataFrame):
            raise EurobaseError('wrong value for METABASE parameter')
        self.__metabase = metabase

    #/************************************************************************/
    @property
    def dimensions(self):
        return self.__dimensions.keys()
    @dimensions.setter
    def dimensions(self, dimensions):
        if isinstance(dimensions, dict):
            dimensions = dimensions
        elif isinstance(dimensions, (list,tuple)):
            dimensions = dict(zip_longest(list(dimensions),None))
        elif isinstance(dimensions, str):
            dimensions = {dimensions: None}
        if not isinstance(dimensions, dict) or not all([isinstance(d,str) for d in dimensions]):
            raise EurobaseError('wrong type for DIMENSIONS parameter')       
        self.__dimensions = dimensions # not an update!

    #/************************************************************************/
    @property
    def datasets(self):
        # return [items for lists in self.__datasets.values() for items in lists]
        return self.__datasets.keys()
    @datasets.setter
    def datasets(self, datasets):
        if isinstance(datasets, dict):
            datasets = datasets
        elif isinstance(datasets, (list,tuple)):
            datasets = dict(zip_longest(list(datasets),None))
        elif isinstance(datasets, str):
            datasets = {datasets: None}
        if not isinstance(datasets, dict) or not all([isinstance(d,str) for d in datasets]):
            raise EurobaseError('wrong type for DATASETS parameter')       
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
        #[kwargs.update({attr: kwargs.get(attr) or getattr(self, '{}'.format(attr))})
        #    for attr in ('query','sort')]
        kwargs.update({'query': kwargs.get('query') or self.query})
        self.__url=Session.build_url(self.domain, **kwargs)
    def getURL(self, **kwargs):
        # update/merge passed arguments with already existing ones
        if 'lang' in  kwargs:   kwargs.pop('lang')
        #[kwargs.update({attr: kwargs.get(attr) or getattr(self, '{}'.format(attr))})
        #    for attr in set(('query','sort')).intersection(set(kwargs.keys()))]
        if kwargs != {}:
            kwargs.update({'query': kwargs.get('query') or self.query})
        return Session.build_url(self.domain, **kwargs) or self.url
    @property
    def url(self):
        #if self._url is None:   self.setURL()
        return self.__url
            
    #/************************************************************************/
    @property
    def all_datasets(self):
        if self.metadata is not None:
            return self.__metadata[settings.BULK_BASE_NAMES[0]].unique().tolist()
        else:
            return self.bulk_datasets
    @property
    def bulk_datasets(self):
        datasets = []
        url = self.__complete_url(self.url, sort=self.sort, dir=settings.BULK_DATA_DIR)
        for alpha in list(string.ascii_lowercase):
            urlalpha = '{url}&start={alpha}'.format(url=url, alpha=alpha)
            html = self.session.load_page(urlalpha)
            if html is None or html == '':
                raise EurobaseError('no HTML content found') 
            _, rows = self.session.read_html_table(html, attrs={'class':'filelist'})
            datasets += [d.replace('.{}'.format(settings.BULK_DATA_EXT),'')     \
                                       for d in self.__filter_table(rows)]
        return datasets
    @property
    def all_dimensions(self):
        if self.metadata is not None:
            return self.__metadata[settings.BULK_BASE_NAMES[1]].unique().tolist()
        else:
            return self.bulk_dimensions
    @property
    def bulk_dimensions(self):
        url = self.__complete_url(self.url, lang=self.lang, sort=self.sort, dir=settings.BULK_DIC_DIR)
        html = self.session.load_page(url)
        if html is None or html == '':
            raise EurobaseError('no HTML content found') 
        _, rows = self.session.read_html_table(html, attrs={'class':'filelist'})
        dimensions = [d.replace('.{}'.format(settings.BULK_DIC_EXT),'')     \
                                for d in self.__filter_table(rows)]
        return dimensions
        
    @staticmethod
    def __complete_url(domain, **kwargs):
        if kwargs == {}:
            return domain
        # set parameters
        if 'lang' in kwargs:    lang = kwargs.pop('lang')
        else:                   lang = None
        if lang is not None and lang not in settings.LANGS:
            raise EurobaseError('language not supported')
        # bug with the API? note that 'sort' needs to be the first item of the 
        # filters
        if 'sort' in kwargs:    sort = kwargs.pop('sort')
        else:                   sort = settings.DEF_SORT
        kwargs = OrderedDict(([('sort',sort)]+list(kwargs.items())))
        # see also https://www.python.org/dev/peps/pep-0468/ 
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
        return self.__check_member(dimension, self.all_dimensions)
    def check_dataset(self, dataset):
        return self.__check_member(dataset, self.all_datasets)
    
    #/************************************************************************/
    def setDatasets(self, dataset):
        self.__datasets.update({dataset: self.getDimensions(self, dataset)})
    def setDimensions(self, dimension, **kwargs):
        self.__dimensions.update({dimension: self.getLabels(self, dimension)})
    @staticmethod
    def __set_member(member, **kwargs):
        pass
    
    #/************************************************************************/
    def getDatasets(self, dimension, **kwargs):
        kwargs.update({'dimension': dimension})
        return self.__get_member('dataset', self.metabase, **kwargs)
    def getDimensions(self, dataset, **kwargs):
        kwargs.update({'dataset': dataset})
        return self.__get_member('dimension', self.metabase, **kwargs)
    def getLabels(self, dimension, **kwargs):
        kwargs.update({'dimension': dimension})
        return self.__get_member('label', self.metabase, **kwargs)
    @staticmethod
    def __get_member(member, metabase, **kwargs):
        if metabase is None:
            raise EurobaseError('metabase data not found')
        members = settings.BULK_BASE_NAMES
        # note that we have BULK_BASE_NAMES = ['dataset', 'dimension', 'label']
        if member not in members:
            raise EurobaseError('member value not recognised - '
                                'must be any string in: {}'.format(members))
        members.pop(member)
        grpby, fltby = [], []
        [grpby.append(m) or fltby.append(kwargs.get(m)) for m in members   \
             if m in kwargs]
        group = metabase.groupby(grpby).get_group(tuple(fltby))
        return group[member].unique().tolist()
     
    #/************************************************************************/
    def setMetabase(self, **kwargs):
        self.__metabase = self.getMetabase(**kwargs)
    #/************************************************************************/
    def getMetabase(self, **kwargs):
        basefile = '{}.{}'.format(settings.BULK_BASE_FILE, settings.BULK_BASE_EXT)
        url = self.__complete_url(self.url, sort=self.sort, file=basefile)
        html = self.session.load_page(url)
        if html is None or html == '':
            raise EurobaseError('no HTML content found') 
        kwargs.update({'header': None, # no effect...
                       'names': self.BULK_BASE_NAMES})
        # !!! it seems there is a problem with compression='infer' since it is 
        # not working well !!!
        dcomp = {'gz': 'gzip', 'bz2': 'bz2', 'zip': 'zip' }
        compression =[dcomp[ext] for ext in dcomp if settings.BULK_BASE_EXT.endswith(ext)][0]
        kwargs.update({'compression': compression})
        # run the pandas.read_table method
        try:
            metabase = self.session.load_file_table(url, **kwargs)
        except:
            metabase = None
        return metabase
