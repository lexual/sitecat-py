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

Python example usage::

    from sitecat_py.python_api import SiteCatPy

    sc_py = SiteCatPandas(username, secret)

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
    ####
    json_data = sc_py.get_trended_report(request_data['report_description'])
