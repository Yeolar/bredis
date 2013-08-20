import json
import base64
import logging
import time
from datetime import datetime, date
from datetime import time as dtime

from tornado.util import ObjectDict


def _encode_key(s):
    try:
        return base64.b64encode(str(s)).replace("\n", "")
    except UnicodeError, e:
        return base64.b64encode(s.encode('utf-8')).replace("\n", "")


def deserialize(s, ignore_error=False):
    try:
        return json.loads(s, object_hook=ObjectDict)
    except Exception as e:
        if ignore_error:
            return s
        logging.error(u'JSON (%s) error: [%s]', s, e)
        return None


def datetime2timestamp(dt):
    return int(time.mktime(dt.timetuple()))


def time2timestamp(t):
    return t.hour * 3600 + t.minute * 60 + t.second


def timestamp2datetime(f):
    return datetime.fromtimestamp(f)


class JSONDateEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, datetime):
            return datetime2timestamp(o)
        elif isinstance(o, dtime):
            return time2timestamp(o)
        return json.JSONEncoder.default(self, o)


def json_date_dumps(obj, **kwargs):
    kwargs['cls'] = JSONDateEncoder
    return json.dumps(obj, **kwargs)

