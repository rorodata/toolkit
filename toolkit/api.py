"""
toolkit.api
~~~~~~~~~~~

Utilities to interact with JSON APIs.
"""
from .imports import lazy_import

requests = lazy_import("requests")

class API:
    """Utility to work with JSON APIs.
    """
    def __init__(self, base_url, auth=None):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.auth = self._parse_auth(auth)

    def _parse_auth(self, auth):
        if isinstance(auth, dict) and 'token' in auth:
            return BearerAuth(auth['token'])
        else:
            return auth

    def get(self, path, params=None, **kwargs):
        return self.request('GET', path, params=params, **kwargs).json()

    def post(self, path, data=None, json=None, **kwargs):
        return self.request('POST', path, data=data, json=json, **kwargs).json()

    def delete(self, path, **kwargs):
        return self.request('POST', path, **kwargs).json()

    def request(self, method, path, **kwargs):
        url = self.base_url + path
        response = self.session.request(method, url, **kwargs)
        response.raise_for_status()
        return response

class BearerAuth:
    def __init__(self, token):
        self.token = token

    def __call__(self, request):
        request.headers['Authorization'] = 'Bearer ' + self.token
        return request
