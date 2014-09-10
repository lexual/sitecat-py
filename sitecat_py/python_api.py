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
        max_tries = 5
        for _ in range(5):
            r = requests.post(self.url, data=data, headers=headers,
                              params=query)
            if r.status_code != 500:
                break
        return r.json()

    def make_report_request(self, method, request_data):
        queued_request = self.make_request(method, request_data)
        status = queued_request['status']
        if status.startswith('error'):
            raise Exception('Invalid request: %s' % queued_request)
        return queued_request['reportID']

    def make_queued_report_request(self, method, request_data,
                                   max_queue_checks=20, queue_check_freq=1):
        """queue request, wait for it to finish, return reponse as json"""
        reportID = self.make_report_request(method, request_data)
        for queue_check in xrange(max_queue_checks):
            time.sleep(queue_check_freq)
            print 'queue check %s' % (queue_check + 1)
            if self.is_report_done(reportID):
                break
        else:
            raise Exception('max_queue_checks reached!!')
        report = self.make_request('Report.GetReport',
                                   {'reportID': reportID})
        return report

    def is_report_done(self, report_id):
        job_status = self.make_request('Report.GetStatus',
                                       {'reportID': report_id})
        status = job_status['status']
        is_done = status == 'done'
        return is_done

    def make_saint_request(self, request_data):
        job_id = self.make_request('Saint.ExportCreateJob', request_data)
        return job_id


    def is_saint_report_done(self, job_id):
        returned_status = self.make_request('Saint.CheckJobStatus',
                                              {'job_id': job_id})
        status = returned_status[0]['status']
        if status == 'Completed':
            _, file_req = returned_status
            file_status = file_req['status']
            if file_status == 'Ready':
                return True
        elif status == 'failed':
            raise Exception('failed: %s' % job_status)
        return False

    def get_saint_report_filesegments(self, job_id):
        returned_status = self.make_request('Saint.CheckJobStatus',
                                            {'job_id': job_id})
        _, file_req = returned_status
        file_id = file_req['id']
        pages = file_req['viewable_pages']
        file_segments = []
        for page in xrange(1, int(pages) + 1):
            file_segment = self.make_request('Saint.ExportGetFileSegment',
                                             {'file_id': file_id,
                                              'segment_id': page})
            file_segments.append(file_segment)
        return file_segments

    def make_queued_saint_request(self, request_data, max_queue_checks=20,
                                  queue_check_freq=1,
                                  queue_only=False):
        """queue request, wait for it to finish, return reponse as json"""
        job_id = self.make_saint_request(request_data)
        if queue_only:
            return job_id
        for queue_check in xrange(max_queue_checks):
            time.sleep(queue_check_freq)
            print 'queue check %s' % (queue_check + 1)
            if self.is_saint_report_done(job_id):
                break
        else:
            raise Exception('max_queue_checks reached!!')
        file_segments = self.get_saint_report_filesegments(job_id)
        return file_segments

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
                   queue_check_freq=None, queue_only=False):
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
        if 'elements' in report_description:
            method = 'Report.QueueTrended'
        else:
            method = 'Report.QueueOvertime'
        if queue_only:
            return self.make_report_request(method, **kwargs)
        else:
            if max_queue_checks:
                kwargs['max_queue_checks'] = max_queue_checks
            if queue_check_freq:
                kwargs['queue_check_freq'] = queue_check_freq
            return self.make_queued_report_request(method, **kwargs)
