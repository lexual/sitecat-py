import datetime
import pandas as pd

from python_api import SiteCatPy


class SiteCatPandas:

    def __init__(self, username, secret, url=None):
        if url:
            self.url = url
        else:
            self.url = 'https://api.omniture.com/admin/1.3/rest/'
        self.username = username
        self.secret = secret
        self.omni = SiteCatPy(username, secret)

    def read_sc(self, report_suite_id, date_from, date_to, metrics,
                date_granularity='day', elements=None,
                max_queue_checks=None, queue_check_freq=None):
        """read data report from SiteCatalyst, return as dataframe."""
        report_description = {
            'reportSuiteID': report_suite_id,
            'dateFrom': iso8601ify(date_from),
            'dateTo': iso8601ify(date_to),
            'dateGranularity': date_granularity,
            'metrics': [{'id': metric} for metric in metrics],
        }
        if elements:
            report_description['elements'] = elements
        return self.read_sc_api(report_description=report_description,
                                max_queue_checks=max_queue_checks,
                                queue_check_freq=queue_check_freq)

    def read_sc_api(self, report_description, max_queue_checks=None,
                     queue_check_freq=None):
        """
        read data report from SiteCatalyst, return as dataframe.

        report_description is a report_description as required by SC API.
        """
        kwargs = {'report_description': report_description}
        if max_queue_checks:
            kwargs['max_queue_checks'] = max_queue_checks
        if queue_check_freq:
            kwargs['queue_check_freq'] = queue_check_freq
        jdata = self.omni.get_report(**kwargs)
        df = self.df_from_sitecat_raw(jdata)
        return df

    # deprecated?!?
    def read_trended(self, report_description, max_queue_checks=None,
                     queue_check_freq=None):
        """read trended data from SiteCatalyst, return as dataframe."""
        kwargs = {'report_description': report_description}
        if max_queue_checks:
            kwargs['max_queue_checks'] = max_queue_checks
        if queue_check_freq:
            kwargs['queue_check_freq'] = queue_check_freq
        jdata = self.omni.get_trended_report(**kwargs)
        df = self.df_from_sitecat_raw(jdata)
        return df

    @classmethod
    def df_from_sitecat_raw(cls, raw_data):
        """
        input: parsed json data that comes from SiteCat's api.
        output: pandas dataframe.
        Currently handles up to 2 levels of element breakdowns.
        """
        data = raw_data['report']['data']
        if len(data) == 0:
            return pd.DataFrame()
        _lower_no_spaces = lambda x: x.replace(' ', '_').lower()
        metrics = [_lower_no_spaces(x['name'])
                   for x in raw_data['report']['metrics']]
        element_names = []
        for element in raw_data['report']['elements']:
            if element['id'] != 'datetime':
                try:
                    element_name = element['classification']
                except KeyError:
                    element_name = element['name']
                element_names.append(_lower_no_spaces(element_name))
        flattened = []
        for days_data in data:
            cls._flatten(flattened, days_data, ())
        # get rid of empty data, with not enough breakdowns.
        flattened = [x for x in flattened
                     if len(x[0]) == len(element_names) + 1]
        # figure out which element is date, which are not.
        first_elements = flattened[0][0]
        for i, element in enumerate(first_elements):
            if hasattr(element, 'year'):
                date_element_n = i
                non_date_ns = [n for n, _ in enumerate(first_elements)
                               if i != n]
                break
        records = []
        for elements, counts in flattened:
            # 1st element is date string, the rest are the breakdowns.
            non_date_elements = [elements[x] for x in non_date_ns]
            date = elements[date_element_n]
            if hasattr(date, 'hour'):
                time_col = 'hour'
            else:
                time_col = 'date'
            record = {time_col: date}
            for i, element in enumerate(non_date_elements):
                record[element_names[i]] = element
            for i, metric_value in enumerate(counts):
                try:
                    record[metrics[i]] = int(metric_value)
                except ValueError:
                    record[metrics[i]] = float(metric_value)
            records.append(record)
        df = pd.DataFrame(records)
        df.set_index(time_col, inplace=True)
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.DatetimeIndex(df.index)
        df.sort_index(inplace=True)
        df.index.name = time_col
        # None's in SiteCat show up as "::unspecified::" in the API.
        for col in element_names:
            df[col].replace({'::unspecified::': 'None'}, inplace=True)
        df = df[element_names + metrics]
        return df

    @classmethod
    def _flatten(cls, results, data, names):
        """
        builds list into results
        parses data recursively
        names is used to keep track of names in the various depths of recursion
            it is a list.
        """
        def _get_name(d):
            """return datetime or date object if date, else the name."""
            if 'hour' in d:
                name = datetime.datetime(d['year'], d['month'],
                                         d['day'], d['hour'])
            elif 'year' in d:
                name = datetime.date(d['year'], d['month'], d['day'])
            else:
                name = data['name']
            return name

        if 'breakdown' not in data:
            name = _get_name(data)
            result = (names + (name,), data['counts'])
            results.append(result)
        else:
            for lower_data in data['breakdown']:
                name = _get_name(data)
                cls._flatten(results, lower_data, names + (name,))


def iso8601ify(date):
    if not isinstance(date, basestring):
        try:
            date = date.date().isoformat()
        except AttributeError:
            date = date.isoformat()
    return date
