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
            self.url = 'https://api.omniture.com/admin/1.4/rest/'
        self.username = username
        self.secret = secret

    def _get_header_auth(self):
        """Creates header information for authentication required by sitecat"""
        nonce = str(time.time())
        created_date = datetime.datetime.utcnow().isoformat()
        created_date = created_date[:18] + 'Z'

        sha1 = hashlib.sha1()
        to_be_hashed = (nonce + created_date + self.secret).encode('ascii')
        sha1.update(to_be_hashed)
        digest = sha1.digest()
        b64_nonce = b64encode(nonce.encode('ascii')).decode('ascii')
        b64_digest = b64encode(digest).decode('ascii')
        auth = {
            'Username': self.username,
            'PasswordDigest': b64_digest,
            'Created': created_date,
            'Nonce': b64_nonce,
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
        for _ in range(3):
            # sometimes, seem to complain about repeated Nonce.
            queued_request = self.make_request(method, request_data)
            if 'error' not in queued_request:
                report_id = queued_request['reportID']
                break
        if 'error' in queued_request:
            raise Exception('Invalid request: %s' % queued_request)
        return report_id

    def make_queued_report_request(self, method, request_data,
                                   max_queue_checks=20, queue_check_freq=1):
        """queue request, wait for it to finish, return reponse as json"""
        reportID = self.make_report_request(method, request_data)
        for queue_check in range(max_queue_checks):
            time.sleep(queue_check_freq)
            print('queue check %s' % (queue_check + 1))
            if self.is_report_done(reportID):
                break
        else:
            raise Exception('max_queue_checks reached!!')
        report = self.make_request('Report.Get',
                                   {'reportID': reportID})
        return report

    def is_report_done(self, report_id):
        job_status = self.make_request('Report.Get',
                                       {'reportID': report_id})
        if 'error' in job_status:
            if job_status['error'] != 'report_not_ready':
            	return None
        is_done = 'report' in job_status
        return is_done

    def make_saint_request(self, request_data):
        job_id = self.make_request('Saint.ExportCreateJob', request_data)
        return job_id


    def is_saint_report_done(self, job_id):
        returned_status = self.make_request('Saint.CheckJobStatus',
                                              {'job_id': job_id})
        status = returned_status[0]['status']
        if status == 'Completed':
            if len(returned_status) == 1:
                return True
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
        if len(returned_status) == 1:
            return []
        _, file_req = returned_status
        file_id = file_req['id']
        pages = file_req['viewable_pages']
        file_segments = []
        for page in range(1, int(pages) + 1):
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
        for queue_check in range(max_queue_checks):
            time.sleep(queue_check_freq)
            print('queue check %s' % (queue_check + 1))
            if self.is_saint_report_done(job_id):
                break
        else:
            raise Exception('max_queue_checks reached!!')
        file_segments = self.get_saint_report_filesegments(job_id)
        return file_segments

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
        
        method = 'Report.Queue'
        if queue_only:
            return self.make_report_request(method, **kwargs)
        else:
            if max_queue_checks:
                kwargs['max_queue_checks'] = max_queue_checks
            if queue_check_freq:
                kwargs['queue_check_freq'] = queue_check_freq
            return self.make_queued_report_request(method, **kwargs)
