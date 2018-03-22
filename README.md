pyrostat
========

Interface to the REST API providing access to Eurostat online database.
---

**About**

This module will enable you to automatically query, search, download and handle data from the [online database](http://ec.europa.eu/eurostat/data/database) of [_Eurostat_](http://ec.europa.eu/eurostat/).

<table align="center">
    <tr> <td align="left"><i>documentation</i></td> <td align="left"><strike>available at: https://eurostat.github.io/pyrostat/</strike></td> </tr> 
    <tr> <td align="left"><i>status</i></td> <td align="left">since 2017 &ndash; <b>in construction</b></td></tr> 
    <tr> <td align="left"><i>contributors</i></td> 
    <td align="left" valign="middle">
<a href="https://github.com/gjacopo"><img src="https://github.com/gjacopo.png" width="40"></a>
</td> </tr> 
    <tr> <td align="left"><i>license</i></td> <td align="left"><a href="https://joinup.ec.europa.eu/sites/default/files/eupl1.1.-licence-en_0.pdfEUPL">EUPL</a> </td> </tr> 
</table>


**<a name="Description"></a>Description**

**<a name="Notes"></a>Notes**

* The Web Services have some limitation as to the supported for a request since currently a maximum of 50 "categories", _e.g._ a message "Too many categories have been requested. Maximum is 50." is returned in case of a too large request (see the data scope and query size limitation [here](http://ec.europa.eu/eurostat/web/json-and-unicode-web-services/data-scope-and-query-size)). This limitation is bypassed by the use of the `esdata` package.

**<a name="Sources"></a>Data sources**

* EU open data initiatives: [pan-European public data infrastructure](http://data.europa.eu).
* Eurostat database: [online catalog](http://ec.europa.eu/eurostat/data/database) and [bulk download facility](http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing).
* Eurostat web-services: access to [JSON and unicode data](http://ec.europa.eu/eurostat/web/json-and-unicode-web-services/about-this-service), the [REST API](http://ec.europa.eu/eurostat/web/json-and-unicode-web-services/getting-started/rest-request) with its [query builder](http://ec.europa.eu/eurostat/web/json-and-unicode-web-services/getting-started/query-builder).
* Eurostat standard code lists: [RAMON](http://ec.europa.eu/eurostat/ramon/nomenclatures/index.cfm?TargetUrl=LST_NOM&StrGroupCode=SCL&StrLanguageCode=EN) metadata.

**<a name="References"></a>Tools and references**

* [**How Open Are Official Statistics?**](http://opendatawatch.com/monitoring-reporting/how-open-are-official-statistics/).
* Lahti L., Huovari J., Kainu M., and Biecek, P. (2017): [**Retrieval and analysis of Eurostat open data with the eurostat package**](https://journal.r-project.org/archive/2017/RJ-2017-019/RJ-2017-019.pdf), _The R Journal_, 9(1):385-392.
* Package [_eurostat_ `R`](http://ropengov.github.io/eurostat) access open data from Eurostat.
* Library [_java4eurostat_](https://github.com/eurostat/java4eurostat) for multi-dimensional data manipulation.
* Lightweight dissemination format [_JSON-stat_](https://json-stat.org).
* Library [_jsonstat.py](https://pypi.python.org/pypi/jsonstat.py) for reading JSON-stat format data.
* Client [_pandaSDMX_](https://pandasdmx.readthedocs.io/en/v0.7.0/) for statistical data and metadata exchange in `Python`.
* Library [_wbdata_](https://github.com/OliverSherouse/wbdata) for accessing World Bank data.

