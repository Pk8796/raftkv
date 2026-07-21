import json
import time
import urllib.request


def _request(url, method, payload, timeout):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
    return json.loads(body) if body else {}


def post_json(url, payload, timeout=2.0, retries=1, backoff=0.2):
    return _with_retry(url, "POST", payload, timeout, retries, backoff)


def get_json(url, timeout=2.0, retries=1, backoff=0.2):
    return _with_retry(url, "GET", None, timeout, retries, backoff)


def _with_retry(url, method, payload, timeout, retries, backoff):
    last = None
    for attempt in range(retries + 1):
        try:
            return _request(url, method, payload, timeout)
        except Exception as e:
            last = e
            if attempt < retries:
                time.sleep(backoff * (2 ** attempt))
    raise last
