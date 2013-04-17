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
        df = df.sort(time_col)
        df.index = pd.DatetimeIndex(df[time_col])
        # None's in SiteCat show up as "::unspecified::" in the API.
        for col in element_names:
            df[col].replace({'::unspecified::': 'None'}, inplace=True)
        return df

    @classmethod
    def _flatten(cls, results, data, names):
        """
        builds list into results
        parses data recursively
        names is used to keep track of names in the various depths of recursion
            it is a list.
        """
        if 'breakdown' not in data:
            try:
                name = datetime.datetime(data['year'], data['month'],
                                         data['day'], data['hour'])
            except KeyError:
                name = datetime.date(data['year'], data['month'], data['day'])

            result = (names + (name,), data['counts'])
            results.append(result)
        else:
            for lower_data in data['breakdown']:
                name = data['name']
                cls._flatten(results, lower_data, names + (name,))
