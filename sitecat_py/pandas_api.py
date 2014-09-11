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

    def read_sc_report(self, report_id):
        """
        Read already queued report by report_id
        """
        jdata = self.omni.make_request('Report.GetReport',
                                       {'reportID': report_id})
        df = self.df_from_sitecat_raw(jdata)
        return df

    def read_sc(self, report_suite_id, date_from, date_to, metrics,
                date_granularity='day', elements=None, segment_id=None,
                max_queue_checks=None, queue_check_freq=None,
                queue_only=False):
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
        if segment_id:
            report_description['segment_id'] = segment_id
        return self.read_sc_api(report_description=report_description,
                                max_queue_checks=max_queue_checks,
                                queue_check_freq=queue_check_freq,
                                queue_only=queue_only)

    def read_sc_api(self, report_description, max_queue_checks=None,
                     queue_check_freq=None, queue_only=False):
        """
        read data report from SiteCatalyst, return as dataframe.

        report_description is a report_description as required by SC API.
        """
        kwargs = {'report_description': report_description}
        if max_queue_checks:
            kwargs['max_queue_checks'] = max_queue_checks
        if queue_check_freq:
            kwargs['queue_check_freq'] = queue_check_freq
        kwargs['queue_only'] = queue_only
        if queue_only:
            report_id = self.omni.get_report(**kwargs)
            return report_id
        else:
            jdata = self.omni.get_report(**kwargs)
            df = self.df_from_sitecat_raw(jdata)
            return df

    def read_saint_api_report(self, job_id):
        file_segments = self.omni.get_saint_report_filesegments(job_id)
        return self._df_from_filesegments(file_segments)

    def _df_from_filesegments(self, file_segments, only_unclassified=False):
        """read SAINT file_segments into a single dataframe"""
        if len(file_segments) == 0:
            return pd.DataFrame()
        dfs = []
        for file_segment in file_segments:
            df = self.df_from_saint_raw(file_segment,
                                        only_unclassified=only_unclassified)
            dfs.append(df)
        return pd.concat(dfs, ignore_index=True)

    def read_saint_api(self, report_description, only_unclassified,
                       max_queue_checks=None, queue_check_freq=None,
                       queue_only=False):
        """
        read data report from SiteCatalyst SAINT, return as dataframe.

        report_description is a report_description as required by Saint API.
        """
        kwargs = {'request_data': report_description}
        kwargs['queue_only'] = queue_only
        if queue_only:
            job_id = self.omni.make_queued_saint_request(**kwargs)
            return job_id
        else:
            if max_queue_checks:
                kwargs['max_queue_checks'] = max_queue_checks
            if queue_check_freq:
                kwargs['queue_check_freq'] = queue_check_freq
            file_segments = self.omni.make_queued_saint_request(**kwargs)
            return self._df_from_filesegments(file_segments, only_unclassified)

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
        try:
            first_elements = flattened[0][0]
        except IndexError:
            # empty report returned
            return pd.DataFrame()
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

    @staticmethod
    def df_from_saint_raw(raw_data, only_unclassified=False):
        rows = [x['row'] for x in raw_data[0]['data']]
        if only_unclassified:
            rows = [row for row in rows if len(row) == 1]
        while len(rows[0]) < len(raw_data[0]['header']):
            rows[0].append(None)
        df = pd.DataFrame(rows, columns=raw_data[0]['header'])
        return df



def iso8601ify(date):
    if not isinstance(date, basestring):
        try:
            date = date.date().isoformat()
        except AttributeError:
            date = date.isoformat()
    return date
