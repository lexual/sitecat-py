import datetime
import hashlib
import json
import requests
import time


from pprint import pprint
from base64 import b64encode


class SiteCatPy:

    def __init__(self, username, secret, url=None):
        """Constructor, sets username and secret credentials"""
        if url:
            self.url = url
        else:
            self.url = 'https://api.omniture.com/admin/1.3/rest/'
        self.username = username
        self.secret = secret

    def _get_header_auth(self):
        """Creates header information for authentication required by sitecat"""
        nonce = str(time.time())
        created_date = datetime.datetime.now().isoformat()

        sha1 = hashlib.sha1()
        sha1.update(nonce + created_date + self.secret)
        digest = sha1.digest()
        auth = {
            'Username': self.username,
            'PasswordDigest': b64encode(digest),
            'Created': created_date,
            'Nonce': b64encode(nonce),
        }
        # looks like: 'UsernameToken Username="foo", PasswordDigest="%s", ...'
        header_auth = 'UsernameToken '
        header_auth += ', '.join(['%s="%s"' % (k, v) for k, v in auth.items()])

        headers = {'X-WSSE': header_auth}
        return headers

    def make_request(self, method, request_data):
        """make a request, return response as json"""
        query = {'method': method}
        headers = self._get_header_auth()
        data = json.dumps(request_data)
        r = requests.post(self.url, data=data, headers=headers, params=query)
        return r.json()

    def make_queued_request(self, method, request_data, max_queue_checks=10,
                            queue_check_freq=1):
        """queue request, wait for it to finish, return reponse as json"""
        queued_request = self.make_request(method, request_data)
        pprint(queued_request)
        status = queued_request['status']
        if status.startswith('error'):
            raise Exception('Invalid request: %s' % queued_request)
        reportID = queued_request['reportID']
        for queue_check in xrange(max_queue_checks):
            time.sleep(queue_check_freq)
            print 'queue check %s' % queue_check + 1
            job_status = self.make_request('Report.GetStatus',
                                           {'reportID': reportID})
            status = job_status['status']
            print 'report', reportID, status
            if status == 'done':
                break
        else:
            raise Exception('max_queue_checks reached!!')
        report = self.make_request('Report.GetReport',
                                   {'reportID': reportID})
        return report

    def get_trended_report(self, report_description, max_queue_checks=None,
                           queue_check_freq=None):
        """Get a trended report, just pass in report_description as dict"""
        kwargs = {
            'request_data': {
                'validate': 1,
                'reportDescription': report_description,
            }
        }
        if max_queue_checks:
            kwargs['max_queue_checks'] = max_queue_checks
        if queue_check_freq:
            kwargs['queue_check_freq'] = queue_check_freq
        return self.make_queued_request('Report.QueueTrended', **kwargs)
