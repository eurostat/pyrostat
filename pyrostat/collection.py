# -*- coding: utf-8 -*-

"""
.. collection.py

Basic class used for the definition and retrieval of online collections, e.g. 
dimensions and datasets, from `Eurostat online database <http://ec.europa.eu/eurostat>`_.

**About**

*credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_ 

*version*:      0.1
--
*since*:        Wed Jan  4 01:57:24 2017

**Description**


**Usage**

    >>> from collection import Collection
    
**Dependencies**

*call*:         :mod:`settings`, :mod:`request`, :mod:`collections`

*require*:      :mod:`os`, :mod:`sys`, :mod:`string`, :mod:`warnings`, \ 
                :mod:`itertools`, :mod:`collections`, :mod:`numpy`
                
*optional*:     :mod:`pandas`, :mod:`lxml`

**Contents**
"""


#==============================================================================
# IMPORT STATEMENTS
#==============================================================================

import os
import warnings
import string
from itertools import zip_longest
from collections import OrderedDict
import copy

import numpy as np

try:
    import dask as dfm # df like DataFrame framework Module
except ImportError:          
    try:
        import pandas as dfm
    except ImportError:          
        class dfm:
            def read_table(*args, **kwargs): 
                raise IOError

try:                                
    import lxml
except ImportError:                 
    pass

from . import pyroWarning, pyroError
from . import settings
from . import session 
# from session import Session

#==============================================================================
# CLASSES/METHODS
#==============================================================================
    
#%%
class __Base(object):
    """Generic collection class.
    """
    #/************************************************************************/
    def __init__(self, **kwargs):
        """Initialisation of a :class:`Collection` instance; pass all domain/query
        items and  set all session.

            >>> coll = Collection(**kwargs)
            
        Keyword Arguments for url query
        -------------------------------
        `domain` : str
            keys used to set various credentials, e.g. key, secret, token, and t
        `query` : str
        `lang` : str
        `sort` : int
        
        Keyword Arguments used for :mod:`session` setting
        -------------------------------------------------  
        `cache`: str
            directory where to store downloaded files
        `time_out`: 
            number of seconds considered to store file on disk, None is infinity, 
            0 for not to store; default
        `force_download` : bool
        """
        # set default values
        self._lang          = settings.DEF_LANG
        self._protocol      = settings.DEF_PROTOCOL
        #self._domain        = ''
        #self._query         = ''
        if kwargs != {}:
            attrs = [a[1] if len(a)>1 else a for a in [attr.split('_') for attr in self.__dict__]] 
            # ( 'domain','query','lang')
            for attr in list(set(attrs).intersection(kwargs.keys())):
                try:
                    setattr(self, '_{}'.format(attr), kwargs.pop(attr))
                except: 
                    warnings.warn(pyroWarning('wrong attribute value {}'.format(attr.upper())))
        self._session       = None
        self._url           = None
        self._status        = None
        self.setSession(**kwargs)   # accepts keywords: time_out, force_download, cache
        self.setMainurl()          # no keywords: for main URL

    #/************************************************************************/
    def getSession(self, **kwargs):
        """Retrieve the session of the :class:`Collection` instance, or define
        a new one when arguments are passed.
        
            >>> session = C._get_session(**kwargs)

        """
        try:
            _session = session.Session(**kwargs)
        except:
            _session = None
        return _session # or self._session
    def setSession(self, **kwargs):
        """Set the session of the :class:`Collection` instance.
        
            >>> session = C._set_session(**kwargs)
        """
        try:
            self._session = self.getSession(**kwargs)
        except:
            raise pyroError('wrong definition for SESSION parameter')
    @property
    def session(self):
        return self._session #.session
              
    #/************************************************************************/
    def _url_static(self, **kwargs):
        # note: kwargs may be empty (i.e. {])})
        # deepcopy instead?
        kwargs.update({'domain':self.domain, 'protocol': self.protocol})
        if 'lang' in  kwargs:   
            kwargs.pop('lang')
        return kwargs
    def _url_dynamic(self, **kwargs):
        return kwargs
   
    #/************************************************************************/
    def build_url(self, **kwargs):
        """Get (build) the URL for the given collection class.
        """
        _kwargs = self._url_static(**kwargs)
        _kwargs = self._url_dynamic(**_kwargs)
        try:
            url = session.Session.build_url(**_kwargs) 
        except:
            url = ''
        return url # or self._url
   
    #/************************************************************************/
    def setMainurl(self, **kwargs):
        """Set the query URL to *Bulk download* web service.
        
            >>> url = C.setMainurl(**kwargs)
           
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
        self._url = self.build_url(**kwargs)
    @property
    def mainurl(self):
        # if self._url is None:   self.setURL()
        return self._url

    #/************************************************************************/
    @property
    def domain(self):
        """Domain attribute (:data:`getter`/:data:`setter`) used through a session 
        launched to connect to bulk data webservice or REST service. 
        """
        return self._domain
    @domain.setter#analysis:ignore
    def domain(self, domain):
        if not isinstance(domain, str):
            raise pyroError('wrong type for DOMAIN parameter')
        self._domain = domain

    #/************************************************************************/
    @property
    def protocol(self):
        """Domain attribute (:data:`getter`/:data:`setter`) used through a session 
        launched to connect to bulk data webservice or REST service. 
        """
        return self._protocol
    @protocol.setter#analysis:ignore
    def protocol(self, protocol):
        if not isinstance(protocol, str):
            raise pyroError('wrong type for PROTOCOL parameter')
        self._protocol = protocol

    #/************************************************************************/
    @property
    def query(self):
        """Query attribute (:data:`getter`/:data:`setter`) attached to the domain
        and used througout a launched session. 
        """
        return self._query
    @query.setter
    def query(self, query):
        if not isinstance(query, str):
            raise pyroError('wrong type for QUERY parameter')
        self._query = query

    #/************************************************************************/
    @property
    def lang(self):
        """Attribute (:data:`getter`/:data:`setter`) defining the language of the
        objects (urls and files) returned when connecting througout a session. 
        See :data:`settings.LANGS` for the list of currently accepted languages. 
        """
        return self._lang
    @lang.setter
    def lang(self, lang):
        if not isinstance(lang, str):
            raise pyroError('wrong type for LANG parameter')
        elif not lang in settings.LANGS:
            raise pyroError('language not supported')
        self._lang = lang

    
#%%
class Bulk(__Base):
    """
    dataset:
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? sort=1&file=data/ilc_di01.tsv.gz
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? sort=1&file=data/aact_ali01.tsv.gz
    metadata:
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? sort=1&file=metadata/aact_esms.sdmx.zip


    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? sort=1&file=dic/en/net_seg10.dic    
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? sort=1&file=dic/en/dimlist.dic    

    dimensions:
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? sort=1&file=dic/en/net_seg10.dic    
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? sort=1&file=dic/en/dimlist.dic    
 
    datasets:
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? dir=data&sort=1&start=a
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? sort=1&file=metabase.txt.gz
    """
    
    #/************************************************************************/
    def __init__(self, **kwargs):
        """Initialisation of a :class:`Collection` instance; pass all domain/query
        items and  set all session.

            >>> x = Collection(**kwargs)
            
        Keyword Arguments
        -----------------  
        `domain` : str
            keys used to set various credentials, e.g. key, secret, token, and t
        `query` : str
        `lang` : str
        `sort` : int
        `dimension` :
        `dataset` :
        
        Keyword Arguments used for :mod:`session` setting
        -------------------------------------------------  
        `cache`: str
            directory where to store downloaded files
        `time_out`: 
            number of seconds considered to store file on disk, None is infinity, 
            0 for not to store; default
        `force_download` : bool
        """
        # set default values
        self._domain        = settings.BULK_DOMAIN
        self._sort          = settings.DEF_SORT
        self._query         = settings.BULK_QUERY
        self._table         = {'dic': {}, 'data': {}}        
        # update
        super(Bulk, self).__init__(**kwargs)
        
    #/************************************************************************/
    @property
    def sort(self):
        """Sort attribute (:data:`getter`/:data:`setter`) used througout a session 
        launched to query _Eurostat_ bulk data collection. 
        """
        return self._sort
    @sort.setter
    def sort(self, sort):
        if not isinstance(sort, int):
            raise pyroError('wrong type for SORT parameter')
        elif not sort > 0:
            raise pyroError('wrong value for SORT parameter')
        self._sort = sort

    #/************************************************************************/
    @property
    def dictionaries(self):
        """Dictionary attribute (:data:`getter`/:data:`setter`) listing all the 
        dimensions (dictionary fields) that have been loaded from the _Eurostat_ 
        bulk download website.
        """
        return self._table['dic'].keys()
    @dictionaries.setter
    def dictionaries(self, dictionaries):
        #if isinstance(dimensions, dict):    do nothing 
        if isinstance(dictionaries, (list,tuple)):
            dictionaries = dict(zip_longest(list(dictionaries),None))
        elif isinstance(dictionaries, str):
            dictionaries = {dictionaries: None}
        if not isinstance(dictionaries, dict) or not all([isinstance(d,str) for d in dictionaries]):
            raise pyroError('wrong type for DICTIONARIES parameter')       
        self._table['dic'] = dictionaries # not an update!
    @property
    def dimensions(self):
        """Dimension attribute is nothing else that a proxy to the dictionary attribute.
        """
        return self.dictionaries
    
    #/************************************************************************/
    @property
    def datasets(self):
        """Dataset attribute (:data:`getter`/:data:`setter`) storing, in a 
        dictionary, the datasets (dictionary fields) that have been loaded from
        the *Eurostat bulk download* website in the :class:`{Collection}` instance.
        """
        # return [items for lists in self.__dataset.values() for items in lists]
        return self._table['data'].keys()
    @datasets.setter
    def datasets(self, datasets):
        # if isinstance(dataset, dict):   do nothing
        if isinstance(datasets, (list,tuple)):
            datasets = dict(zip_longest(list(datasets),None))
        elif isinstance(datasets, str):
            datasets = {datasets: None}
        if not isinstance(datasets, dict) or not all([isinstance(d,str) for d in datasets]):
            raise pyroError('wrong type for DATASETS parameter')       
        self._table['data'] = datasets # not an update!
 
    #/************************************************************************/
    def _url_dynamic(self, **kwargs):
        """Build (update) an URL using a predefined URL (e.g., a domain) and a set
        of query arguments encoded as key/value pairs.
        
            >>> url = C._url_dynamic(**kwargs)
         
        Argument
        --------
        url : str
            basic url path to be extended.
           
        Keyword Arguments
        -----------------  
        kwargs : dict
            any other regularly query arguments to be encoded in the url path, e.g.,
            in something like :data:`key=value` where :data:`key` and :data:`value` 
            are actually the key/value pairs of :data:`kwargs`; among other possible
            queries, :data:`sort` and :data:`lang` are accepted; note that :data:`lang` 
            is used to set the language subdomain; when passed (i.e. :data:`lang` is
            not :data:`None`), the path of the url will be extended with the language
            value: :data:`url/lang`.
            
        Returns
        -------
        url : str
            url path of the form :data:`domain/{query}
            
        See also
        --------
        :meth:`Session.build_url`
        """
        # note: kwargs may be empty (i.e. {])})
        # deepcopy instead?
        if 'query' not in kwargs and self.query not in (None,''):
            kwargs.update({'query': self.query})
        # bug with the API? note that 'sort' needs to be the first item of the 
        # filters
        sort = kwargs.pop('sort', self.sort or settings.DEF_SORT)
        #[_kwargs.update({attr: _kwargs.get(attr) or getattr(self, '{}'.format(attr))})
        #    for attr in set(('query','sort')).intersection(set(_kwargs.keys()))]
        kwargs = OrderedDict(([('sort',sort)]+list(kwargs.items())))
        # see also https://www.python.org/dev/peps/pep-0468/ 
        return kwargs

    #/************************************************************************/
    def build_url(self, **kwargs):
        lang = kwargs.pop('lang', None)
        url = super(Bulk,self).build_url(**kwargs)
        if lang is not None:
            url = "{url}/{lang}".format(url=url,lang=lang)
        return url

    #/************************************************************************/
    def read_html_table(self, key, **kwargs):
        if not isinstance(key, str) or not key.lower() in ('dic','data'):
            raise pyroError('keyword parameter %s not recognised' % key)
        alpha = kwargs.get('alpha')
        if not (alpha is None or alpha in list(string.ascii_lowercase)):
            raise pyroError('wrong parameter ALPHA')
        try:
            table = self._table.get(key)
        except:
            table = {}
        finally:
            if alpha is not None:            
                try:
                    table = table[alpha]
                except:
                    table.update({alpha:None})
                    table = table[alpha]
        if table in (None,{}):
            bulk_dir = settings.BULK_DIR[key]
            if key.lower() == 'dic':
                url = self.build_url(dir=bulk_dir, lang=self.lang)
            elif key.lower() == 'data':
                url = self.build_url(dir=bulk_dir, start=alpha)        
            _kwargs = {'skiprows': [1], 'header': 0}
            try:
                table = self.session.read_html_table(url, **_kwargs)
            except:
                pass
        return table

    #/************************************************************************/
    def read(self, **kwargs):
        """
        dimension example:
        example http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=dic/en/accident.dic

        dataset example: 
        http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? sort=1&file=data/aact_ali01.tsv.gz
        http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data/aact_ali01.sdmx.zip     
        """
        dimension, dataset = kwargs.pop('dic', None), kwargs.pop('data', None)
        if dataset is None and dimension is None:
            raise pyroError('one of the parameters DIC or DATA needs to be set')
        elif not(dataset is None or dimension is None):
            raise pyroError('parameters DIC or DATA are incompatible')
        elif dimension is not None:
            key, entity = 'dic', 'dimension'
        else: # if dataset is not None:
            key, entity = 'data', 'dataset'
        bulk_zip = settings.BULK_ZIP[key]
        bulk_dir = settings.BULK_DIR[key]
        bulk_exts = settings.BULK_EXTS[key]
        ext = kwargs.pop('ext', bulk_exts[0])
        if not ext in bulk_exts:   
            raise pyroError('bulk %s extension EXT not recognised' % key) 
        if bulk_zip != '':
            ext = '%s.%s' % (ext, bulk_zip)
        try:
            resp = getattr(self, 'check_%s' % entity)(dimension or dataset)
        except: 
            pass
        else:
            if resp is False:   raise pyroError('wrong %s' % key) 
        if dimension is not None:
            filename = '%s/%s/%s.%s' % (bulk_dir, self.lang, dimension or dataset, ext)
        else:
            filename = '%s/%s.%s' % (bulk_dir, dimension or dataset, ext)
        url = self.build_url(file=filename)
        return self.session.read_url_page(self, url, **kwargs)

    #/************************************************************************/
    def last_update(self, **kwargs):
        """Retrieve the time a table (dictionary or dataset) was last updated.
        """
        dimension, dataset = [kwargs.get(key) for key in ('dic','data')]
        if dataset is None and dimension is None:
            raise pyroError('one of the parameters DIC or DATA needs to be set')
        elif not(dataset is None or dimension is None):
            raise pyroError('parameters DIC or DATA are incompatible')
        if dimension is not None:
            df = self.load_table('dic')
            kname, kdate = [settings.BULK_DIC_NAMES.get(key) for key in ('Name','Date')]
        else:
            df = self.load_table('data', alpha=dataset[0])
            kname, kdate = [settings.BULK_DATA_NAMES.get(key) for key in ('Name','Date')]
        try:
            names = [d.split('.')[0] for d in list(df[0][kname])]
            dates = [d.split('.')[0] for d in list(df[0][kdate])]
        except:
            raise pyroError('impossible to read {}/{} columns of bulk table'.format(kname,kdate)) 
        try:
            ipar = names.index(dataset or dimension)
        except:
            raise pyroError('entry {} not found in bulk table'.format(dataset or dimension)) 
        else:
            date = dates[ipar]
        return date

    #/************************************************************************/
    @property
    def data_in_table(self):
        datasets = []
        # url = self.update_url(self.url, sort=self.sort, dir=settings.BULK_DATA_DIR)
        # kwargs = {'skiprows': [1], 'header': 0}
        for alpha in list(string.ascii_lowercase):
            try:
                df = self.read_html_table('data', alpha=alpha)
                assert df is not None
                name = settings.BULK_NAMES['data']['name']
            except:
                warnings.warn(pyroWarning('impossible to read html table: %s' % alpha))
            else:
                # note the call to df[0] since there is one table only in the page
                datasets += [d.split('.')[0] for d in list(df[0][name])]
        return datasets

    #/************************************************************************/
    @property
    def dic_in_table(self):
        try:
            df = self.read_html_table('dic')
            assert df is not None
            name = settings.BULK_NAMES['dic']['name']
        except:
            raise pyroError('impossible to read column of bulk table') 
        else:
            # note the call to df[0] since there is one table only in the page
            dictionaries = [d.split('.')[0] for d in list(df[0][name])]
        return dictionaries

    def __obsolete_get_datasets(self):
        datasets = []
        url = self.build_url(dir=settings.BULK_DIR['data'])
        for alpha in list(string.ascii_lowercase):
            urlalpha = '{url}&start={alpha}'.format(url=url, alpha=alpha)
            _, html = self.session.__obsolete_load_page(urlalpha)
            if html is None or html == '':
                raise pyroError('no HTML content found') 
            _, rows = self.session.read_soup_table(html, attrs={'class':'filelist'})
            datasets += [d.split('.')[0] for d in self.__obsolete_filter_table(rows)]
        return datasets
    def __obsolete_get_dimensions(self):
        url = self.build_url(lang=self.lang, dir=settings.BULK_DIR['dic'])
        _, html = self.session.__obsolete_load_page(url)
        if html is None or html == '':
            raise pyroError('no HTML content found') 
        _, rows = self.session.read_soup_table(html, attrs={'class':'filelist'})
        dimensions = [d.split('.')[0] for d in self.__obsolete_filter_table(rows)]
        return dimensions
    @staticmethod
    def __obsolete_filter_table(rows):
        rows = rows[0] # only one table in the page
        data, i = [], 0
        for row in rows:
            i = i+1
            try:                        cols = row.find_all("td")
            except:                     cols = row.findAll("td")
            if cols == [] or i <= 2:    continue
            else:                       data.append(cols[0].find('a').find(text=True))
        return data    
    
        
class Meta(__Base):
    """
    metabase:
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? sort=1&file=metabase.txt.gz
    toc:
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? sort=1&file=table_of_contents_en.txt
    dimension (dictionary):
    http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/ BulkDownloadListing? sort=1&file=dic/en/age.dic
    """
 
    #/************************************************************************/
    def __init__(self, **kwargs):
        """Initialisation of a :class:`Meta` instance.
        """
        # set default values
        self._domain        = settings.BULK_DOMAIN
        self._sort          = settings.DEF_SORT
        self._query         = settings.BULK_QUERY
        self._metabase      = {}        
        # update
        super(Bulk, self).__init__(**kwargs)

    #/************************************************************************/
    @property
    def metabase(self):
        """Metabase attribute (:data:`getter`/:data:`setter`) storing, in a table,
        the information of _Eurostat_ bulk download metabase.
        """
        return self._metabase
    @metabase.setter
    def metabase(self, metabase):
        if isinstance(metabase, np.array):
            if dfm is not None:
                self._metabase = dfm.DataFrame(data=metabase)
            else:
                pass
        elif not isinstance(metabase, dfm.DataFrame):
            raise pyroError('wrong value for METABASE parameter')
 
    #/************************************************************************/
    @property
    def toc(self):
        """TOC attribute (:data:`getter`/:data:`setter`) storing, the table of 
        contents hosted on _Eurostat_ bulk download metabase.
        """
        return self._toc
    @toc.setter
    def toc(self, toc):
        if isinstance(toc, np.array):
            if dfm is not None:
                self._toc = dfm.DataFrame(data=toc)
            else:
                pass
        elif not isinstance(toc, dfm.DataFrame):
            raise pyroError('wrong value for TOC parameter')
        
    #/************************************************************************/
    @property
    def datasets(self):
        if self.metabase is None:
            raise pyroError('no METABASE data found') 
        key = settings.BULK_NAMES['base']['data']
        return self.metabase[key].unique().tolist()

    #/************************************************************************/
    @property
    def dictionaries(self):
        if self.metabase is None:
            raise pyroError('no METABASE data found') 
        key = settings.BULK_NAMES['base']['dic']
        return self.metabase[key].unique().tolist()
    @property
    def dimensions(self):
        """Dimension attribute is nothing else that a proxy to the dictionary attribute.
        """
        return self.dictionaries
    
    #/************************************************************************/
    def __getitem__(self, item):
        if not isinstance(item, str):
            raise pyroError('wrong type for ITEM parameter')
        elif not (item in self.dictionaries or item in self.datasets):
            raise pyroError('ITEM not recognised as either a dataset or a dictionary')            
        if item in self.dictionary:
            return self._dictionary[item]
        elif item in self.dataset:
            return self._dataset[item]
        else:
            return None
    def __setitem__(self, item, value):
        if not isinstance(item, str):
            raise pyroError('wrong type for ITEM parameter')
        elif not (item in self.dictionaries or item in self.datasets):
            raise pyroError('ITEM not recognised as either a dataset or a dictionary')            
        if item in self.dictionary:
            self._dictionary[item] = value
        elif item in self.dataset:
            self._dataset[item] = value
    def __contains__(self, item):
        if not isinstance(item, str):
            raise pyroError('wrong type for ITEM parameter')
        elif not (item in self.dictionaries or item in self.datasets):
            raise pyroError('ITEM not recognised as either a dataset or a dictionary')            
        return item in self.dictionary or item in self.dataset

    #/************************************************************************/
    @staticmethod
    def __check_member(member, members):
        if members is None or members==[]:
            raise pyroError('no member to perform comparison with')
        if member in members:      
            return True
        else:                       
            return False
    def check_dictionary(self, dictionary):
        return self.__check_member(dictionary, self.dictionaries)
    def check_dataset(self, dataset):
        return self.__check_member(dataset, self.datasets)
    def check(self, item):
        """
        See also :method:`__contains__` method...
        """
        if not isinstance(item, str):
            raise pyroError('wrong type for ITEM parameter')
        elif not (item in self.dictionaries or item in self.datasets):
            raise pyroError('ITEM not recognised as either a dataset or a dictionary')            
        return self.check_dataset(item) or self.check_dictionary(item)
    
    #/************************************************************************/
    @staticmethod
    def __get_member(member, metabase, **kwargs):
        if metabase is None:
            raise pyroError('metabase data not found - get the file from Eurobase')
        members = settings.BULK_NAMES['base'] # ('data', 'dic', 'label')
        if member not in members:
            raise pyroError('member value not recognised - '
                                'must be any string in: {}'.format(members.keys()))
        elif set(kwargs.keys()).intersection([member]) != set(): # not empty
            raise pyroError('member value should not be passed as a keyword argument')
        grpby = list(set(kwargs.keys()).intersection(set(members)))
        if grpby != []:
            fltby = tuple([kwargs.get(k) for k in grpby]) # preserve the order
            if len(grpby) == 1:
                grpby, fltby = grpby[0], fltby[0]
            group = metabase.groupby(grpby).get_group(fltby)
        else:
            group = metabase
        return group[member].unique().tolist()
    @staticmethod
    def __set_member(member, **kwargs):
        pass
    
    #/************************************************************************/
    def getDataset(self, dataset):
        return self.__get_member('dic', self.metabase, data=dataset)
    def getDictionary(self, dimension):
        return self.__get_member('label', self.metabase, dic=dimension)
    def setDataset(self, dataset):
        self._dataset.update({dataset: self.getDataset(dataset)})
    def setDictionary(self, dimension):
        self.__dimensions.update({dimension: self.getDimension(dimension)})

    #/************************************************************************/
    def getAllDatasets(self, dimension = None):
        """Retrieve all the datasets that are using a given dimension.
        """
        if dimension is None:
            return self.__get_member('data', self.metabase)
        else:
            return self.__get_member('data', self.metabase, dic=dimension)
    def getAllDimensions(self, dataset):
        """Retrieve all the dimensions used to define a given dataset.
        """
        return self.__get_member('dic', self.metabase, data=dataset)
    def getAllLabels(self, dimension, **kwargs):
        """Retrieve all the labels of a given dimension and possibly used
        by a given dataset.
        """
        return self.__get_member('label', self.metabase, dic=dimension, **kwargs)
 
    #/************************************************************************/
    def checkDataset(self, dataset):
        """Check whether a dataset exists in Eurostat database.
        
        Argument
        --------
        name : str
            string defining a dataset.
            
        Returns
        -------
        res : bool
            boolean answer (`True`/`False`) to the existence of the dataset `name`.
        """
        # return dataset in self.getAllDatasets(dimension)
        return dataset in self.getAllDatasets()
    def checkDimensionInDataset(self, dimension, dataset):
        """Check whether some dimension is used by a given dataset.
        """
        # return dataset in self.getAllDatasets(dimension)
        return dimension in self.getAllDimensions(dataset)
    def checkLabelInDimension(self, label, dimension, **kwargs):
        """Check whether some label is used by a given dimension.
        """
        return label in self.getAllLabels(dimension, **kwargs)
        
    #/************************************************************************/
    @property
    def geocode(self):
        return self.getDimension('geo')
        
    #/************************************************************************/
    def setMetabase(self, **kwargs):
        self.__metabase = self.readMetabase(**kwargs)
    def readMetabase(self, **kwargs):
        basefile = '{base}.{ext}'.format(base=settings.BULK_FILES['base'], ext=settings.BULK_EXTS['base'])
        if settings.BULK_ZIP['base'] != '':
            basefile = '{base}.{zip}'.format(base=basefile, zip=settings.BULK_ZIP['base'])
        url = self.update_url(self.url, sort=self.sort, file=basefile)
        kwargs.update({'header': None, # no effect...
                       'names': settings.BULK_NAMES['base']})
        # it seems there is a problem with compression='infer' since it is 
        # not working well !!!
        dcomp = {'gz': 'gzip', 'bz2': 'bz2', 'zip': 'zip'}
        #compression =[dcomp[ext] for ext in dcomp if settings.BULK_BASE_EXT.endswith(ext)][0]
        kwargs.update({'compression': dcomp[settings.BULK_ZIP['base']]})
        # run the pandas.read_table method
        try:
            metabase = self.session.read_url_table(url, **kwargs)
        except:
            metabase = None
        return metabase

    #/************************************************************************/
    def search(self, regex):
        mask = np.column_stack([self.metabase[col].str.contains(regex, na=False) \
                                for col in self.metadata])
        res = self.metabase.loc[mask.any(axis=1)]
        return res
        
    #/************************************************************************/
    def setTOC(self, **kwargs):
        self.__toc = self.readToc(**kwargs)
    def readToc(self, **kwargs):
        """
        Example: http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=table_of_contents.xml
        """
        ext = kwargs.pop('ext', settings.BULK_EXTS['toc'][0])
        if ext not in settings.BULK_EXTS['toc']:   
            raise pyroError('bulk table of contents extension EXT not recognised') 
        try:
            lang = kwargs.pop('lang')
        except:
            lang = self.lang
        else:
            if lang not in settings.LANGS:   
                raise pyroError('language LANG not recognised') 
        if ext == 'xml':
            tocfile = '{toc}.xml'.format(toc=settings.BULK_FILES['toc'])
        else:
            tocfile = '{toc}_{lang}.{ext}'.format(toc=settings.BULK_FILES['toc'], lang=lang, ext=ext)
        if settings.BULK_ZIP['toc'] != '':
            tocfile = '{toc}.{zip}'.format(toc=tocfile, zip=settings.BULK_ZIP['toc'])
        url = self.update_url(self.url, sort=self.sort, file=tocfile)
        kwargs.update({'header': 0})
        try:
            if ext == 'xml':
                toc = self.session.read_html_table(url, **kwargs)                
                toc = toc[0]
            else:
                toc = self.session.read_url_table(url, **kwargs)
        except:
            toc = None
        else:
            toc.drop(toc.columns[-1], axis=1, inplace=True) # toc.columns[-1] is 'values'
            toc.applymap(lambda x: x.strip())
        return toc
         
    #/************************************************************************/
    @staticmethod
    def __get_content(member, toc, **kwargs):
        if toc is None:
            raise pyroError('table of contents not found - load the file from Eurobase')
        code = toc['code']
        if code[code.isin([member])].empty:
            raise pyroError('member not found in codelist of table of contents')
        return toc[code==member]
        
    #/************************************************************************/
    def getTitle(self, dataset, **kwargs):
        res = self.__get_content(dataset, self.toc, **kwargs)
        ind = res.index.tolist()
        return res['title'][ind[0]].lstrip().rstrip()
        
    #/************************************************************************/
    def getPeriod(self, dataset, **kwargs):
        res = self.__get_content(dataset, self.toc, **kwargs)
        ind = res.index.tolist()
        start = res['data start'][ind[0]]
        end = res['data end'][ind[0]]
        return [start, end]
    

class REST(__Base):
    """
rest
{host_url}/rest/data/{version}/{format}/{language}/{datasetCode}?{filters}
http://ec.europa.eu/eurostat/wdds/ rest/data/v2.1/ json/en/ nama_gdp_c?precision=1&geo=EU28&unit=EUR_HAB&time=2010&time=2011&indic_na=B1GM
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
                warnings.warn(pyroWarning('wrong attribute value {}'.format(attr.upper())))

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
            raise pyroError('wrong type for FMT parameter')
        elif not fmt in settings.API_FMTS:
            raise pyroError('format not supported')
        self.__fmt = fmt
           
    #/************************************************************************/
    @property
    def lang(self):
        return self.__lang
    @lang.setter
    def lang(self, lang):
        if not isinstance(lang, str):
            raise pyroError('wrong type for LANG parameter')
        elif not lang in settings.API_LANGS:
            raise pyroError('language not supported')
        self.__lang = lang
      
    #/************************************************************************/
    @property
    def vers(self):
        return self.__vers     
    @vers.setter
    def vers(self,version):
        if not(version is None or isinstance(version,float)): 
            raise pyroError('wrong format/unrecognised version')
        elif version < settings.API_VERSION:           
            raise pyroError('version not supported') 
        self.__vers = version
      
    #/************************************************************************/
    @property
    def domain(self):
        return self.__domain     
    @domain.setter
    def domain(self,domain):
        if not(domain is None or isinstance(domain,str)):    
            raise pyroError('wrong format/unrecognised domain') 
        self.__domain = domain
      
    #/************************************************************************/
    @staticmethod
    def _check_dataset(dataset):
        if not(dataset is None or isinstance(dataset,str)):   
            raise pyroError('wrong format/unrecognised dataset') 
        # CHECK DIMENSIONS!!!
        # elif dataset < VERS_REST:            raise pyroError('version not supported')
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
            raise pyroError('wrong format/unrecognised precision')
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
        if status is None:                      raise pyroError('unknown status')
        elif not isinstance(status, int):       raise pyroError('unrecognised status')
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
            raise pyroError('Request URL not (re)set')
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
            raise pyroError('uncomplete information for building URL')
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



class NUTS(__Base):
    pass