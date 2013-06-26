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

    def make_queued_report_request(self, method, request_data,
                                   max_queue_checks=20, queue_check_freq=1):
        """queue request, wait for it to finish, return reponse as json"""
        queued_request = self.make_request(method, request_data)
        pprint(queued_request)
        status = queued_request['status']
        if status.startswith('error'):
            raise Exception('Invalid request: %s' % queued_request)
        reportID = queued_request['reportID']
        for queue_check in xrange(max_queue_checks):
            time.sleep(queue_check_freq)
            print 'queue check %s' % (queue_check + 1)
            job_status = self.make_request('Report.GetStatus',
                                           {'reportID': reportID})
            status = job_status['status']
            print 'report', reportID, status
            if status == 'done':
                break
            elif status == 'failed':
                raise Exception('failed: %s' % job_status)
        else:
            raise Exception('max_queue_checks reached!!')
        report = self.make_request('Report.GetReport',
                                   {'reportID': reportID})
        return report

    def make_queued_saint_request(self, request_data, max_queue_checks=20,
                                  queue_check_freq=1):
        """queue request, wait for it to finish, return reponse as json"""
        job_id = self.make_request('Saint.ExportCreateJob', request_data)
        for queue_check in xrange(max_queue_checks):
            time.sleep(queue_check_freq)
            print 'queue check %s' % (queue_check + 1)
            returned_status = self.make_request('Saint.CheckJobStatus',
                                                  {'job_id': job_id})
            pprint(returned_status)
            status = returned_status[0]['status']
            if status == 'Completed':
                job_req, file_req = returned_status
                file_id = file_req['id']
                file_status = file_req['status']
                print 'File Status for %s is %s' % (file_id, file_status)
                if file_status == 'Ready':
                    status = 'done'
                    pages = file_req['viewable_pages']
            if status == 'done':
                break
            elif status == 'failed':
                raise Exception('failed: %s' % job_status)
        else:
            raise Exception('max_queue_checks reached!!')

        report = self.make_request('Saint.ExportGetFileSegment',
                                   {'file_id': file_id, 'segment_id': 1})
        return report

    # deprecated?
    def get_trended_report(self, report_description, max_queue_checks=None,
                           queue_check_freq=None):
        """Get a trended report, just pass in report_description as dict"""
        if 'elements' not in report_description:
            raise Exception('Trended reports need "elements" defined')
        return self.get_report(report_description=report_description,
                               max_queue_checks=max_queue_checks,
                               queue_check_freq=queue_check_freq)

    def get_report(self, report_description, max_queue_checks=None,
                   queue_check_freq=None):
        """
        Get a report, just pass in report_description as dict

        Will figure out whether to make a Trended or Overtime report.
        """
        kwargs = {
            'request_data': {
                # validation lies, and throws errors, when a valid report
                #   would otherwise have been returned !?!?!?
                'validate': 0,
                'reportDescription': report_description,
            }
        }
        if max_queue_checks:
            kwargs['max_queue_checks'] = max_queue_checks
        if queue_check_freq:
            kwargs['queue_check_freq'] = queue_check_freq
        if 'elements' in report_description:
            method = 'Report.QueueTrended'
        else:
            method = 'Report.QueueOvertime'
        return self.make_queued_report_request(method, **kwargs)
