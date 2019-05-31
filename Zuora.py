import json
import requests


def _unpack_response(operation, path, response):
    if path != '/object/invoice/':
        assert response.status_code == 200, f'{operation} to {path} failed: {response.content}'
    if path.startswith('/files/'):
        return response.text

    return json.loads(response.text)

class Zuora:

    def __init__(self, headers, endpoint='production'):
        self.session = requests.Session()
        if endpoint == 'production':
            self.endpoint = 'https://rest.zuora.com/v1'
        elif endpoint == 'sandbox':
            self.endpoint = 'https://rest.apisandbox.zuora.com/v1'
        else:
            self.endpoint = endpoint

        self.headers = headers

    def _get(self, path, payload=None):
        response = self.session.get(self.endpoint + path,
                                    headers=self.headers,
                                    params=payload,
                                    timeout=360)
        return _unpack_response('GET', path, response)

    def _post(self, path, payload):
        response = self.session.post(self.endpoint + path,
                                     json=payload,
                                     headers=self.headers,
                                     timeout=360)
        return _unpack_response('POST', path, response)

    def query(self, query_string):
        response = self._post("/action/query", {"queryString": query_string})
        return response

    def query_all(self, query_string):
        records = []
        response = self.query(query_string)
        records += response['records']

        while not response['done']:
            response = self.query_more(response['queryLocator'])
            records += response['records']

        return records

    def query_more(self, query_locator):
        return self._post("/action/queryMore", {"queryLocator": query_locator})
