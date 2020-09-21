import time

from requests import Request, Session
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
DEFAULT_TIMEOUT = 10
SUPPORTED_METHOD = ['GET', 'POST']


class DummyResponse(object):
    def __init__(self, content='', json=None, status_code=404):
        if json is None:
            json = dict()
        self.json = json
        self.content = content
        self.status_code = status_code


class HttpRequest(object):
    def __init__(self, wait_interval=None):
        self._wait_interval = wait_interval

    @staticmethod
    def default_headers():
        return {'Accept': 'application/json',
                'User-Agent': "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:34.0)"}

    @staticmethod
    def form_headers():
        return {'Content-type': 'application/x-www-form-urlencoded',
                'User-Agent': "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:34.0)"}

    @property
    def wait_interval(self):
        return self._wait_interval

    @wait_interval.setter
    def wait_interval(self, new_interval):
        self._wait_interval = new_interval

    def send_data(self, method, url, verify=False, **kwargs):
        if self._wait_interval:
            time.sleep(self.wait_interval)

        if method.upper() not in SUPPORTED_METHOD:
            return None

        with Session() as s:
            try:
                req = Request(method, url=url, **kwargs)
                prepared = req.prepare()
                resp = s.send(prepared, verify=verify, timeout=DEFAULT_TIMEOUT)
            except Exception as e:
                resp = DummyResponse()
                resp.content = str(e)
            finally:
                return resp


def get_stock_dates():
    url = "https://raw.githubusercontent.com/chihyi-liao/stock-data/master/date_data.json"
    result = dict()
    req = HttpRequest()
    resp = req.send_data(method='GET', url=url)
    if resp.status_code == 200:
        result = resp.json()
    return result


def convert_to_float(string):
    # noinspection PyBroadException
    try:
        data = ''.join(string.split(','))
        return float(data)
    except Exception:
        return None


def convert_to_int(string):
    # noinspection PyBroadException
    try:
        data = ''.join(string.split(','))
        return int(data)
    except Exception:
        return None


def convert_to_direction(direction):
    if direction == ' ':
        return 0
    else:
        if '-' in direction:
            return -1
        return 1


def convert_change_to_string(sign_change):
    # noinspection PyBroadException
    try:
        num = float(sign_change)
        if num > 0:
            return '+', str(num)
        elif num < 0:
            return '-', str(num)
        else:
            return ' ', '0.0'
    except Exception:
        return ' ', '0.0'
