Python & Pandas support for SiteCatalyst, part of Adobe's Marketing Cloud.

Python support for the SiteCatalyst API.

Pandas integration for pulling reports directly into Pandas DataFrame's.

Pandas example usage::

    from sitecat_py.pandas_api import SiteCatPandas
    
    username = 'my_username'
    secret = 'my_shared_secret''
    
    report_description = {
        "reportSuiteID": "my_reportsuite",
        "dateFrom": "2013-04-01",
        "dateTo": "2013-04-03",
        "dateGranularity": "day",
        "metrics": [{"id": "visits"}],
        "elements": [{"id": "page"}],
        #"elements": [{"id": "product", "classification": "Product Category"}],
    }
    
    sc_pd = SiteCatPandas(username, secret)
    df = sc_pd.read_trended(report_description)
    print df.head()

    >                   date product_category  visits
    > 2013-04-01  2013-04-01             None   13172
    > 2013-04-01  2013-04-01           Shirts    1583
    > 2013-04-01  2013-04-01            Pants     398
    > 2013-04-01  2013-04-01            Jocks     102
    > 2013-04-02  2013-04-02             None   16717

Python example usage::

    from sitecat_py.python_api import SiteCatPy
    from sitecat_py.pandas_api import SiteCatPandas

    sc_py = SiteCatPy(username, secret)

    method = 'ReportSuite.GetSegments'
    request_data = {
        "rsid_list": ["my_reportsuite"],
    }
    json_data = sc_py.make_request(method, request_data)

    ####
    request_data = {
        report_description = {
            "reportSuiteID": "my_reportsuite",
            "dateFrom": "2013-04-01",
            "dateTo": "2013-04-03",
            "dateGranularity": "day",
            "metrics": [{"id": "visits"}],
            "elements": [{"id": "page"}],
            #"elements": [{"id": "product", "classification": "Product Category"}],
        },
        'validate': 1,
    }
    json_data = sc_py.make_queued_request('Report.QueueTrended', request_data)
    df = SiteCatPandas.df_from_sitecat_raw(json_data)
    ####
    json_data = sc_py.get_trended_report(request_data['report_description'])
