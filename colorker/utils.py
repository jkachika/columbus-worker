import collections
import math
import time
from datetime import datetime as dt

import numpy


def current_time_millis():
    return int(round(time.time() * 1000))


# noinspection PyBroadException
def caught(try_function, *args):
    try:
        try_function(*args)
        return False
    except BaseException:
        return True


def is_number(s):
    return False if caught(float, s) else True


# finds the mean of a feature collection for a given property
def mean(prop, ftc):
    features = ftc['features']
    result = [(float(feature['properties'][prop]) if is_number(feature['properties'][prop]) and not math.isnan(
        float(feature['properties'][prop])) else 0.0) for feature in features]
    return numpy.mean(numpy.array(result))


# finds the standard deviation of a feature collection for a given property
def std(prop, ftc):
    features = ftc['features']
    result = [(float(feature['properties'][prop]) if is_number(feature['properties'][prop]) and not math.isnan(
        float(feature['properties'][prop])) else 0.0) for feature in features]
    return numpy.std(numpy.array(result))


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code.
       Add other types that are not serializable
       :param obj: The object that needs to be serialized
       :return: json serialization of the given object
    """
    if isinstance(obj, dt):
        s = obj.strftime('%Y-%m-%d %H:%M:%S.%f')
        tail = s[-7:]
        f = round(float(tail), 3)
        temp = "%.3f" % f
        return "%s%s" % (s[:-7], temp[1:])
    if hasattr(obj, '__dict__'):
        return obj.__dict__
    raise TypeError("Type not serializable")


def deep_update(source, overrides):
    """Update a nested dictionary or similar mapping.
    Modify ``source`` in place.
    :param source - a dictionary that needs to be updated
    :param overrides - a dictionary that provides the new keys and values
    """
    for key, value in overrides.iteritems():
        if isinstance(value, collections.Mapping) and value:
            returned = deep_update(source.get(key, {}), value)
            source[key] = returned
        else:
            source[key] = overrides[key]
    return source


def lists_to_html_table(a_list):
    header = "<tr><th>%s</th></tr>" % ("</th><th>".join(a_list[0]))
    body = ""
    if len(a_list) > 1:
        for sub_list in a_list[1:]:
            body += "<tr><td>%s</td></tr>\n" % ("</td><td>".join(sub_list))
    return "<table>%s\n%s</table>" % (header, body)


def dicts_to_html_table(a_list):
    keys = sorted(a_list[0].keys())
    header = "<tr><th>%s</th></tr>" % ("</th><th>".join(keys))
    body = ""
    if len(a_list) > 1:
        for sub_dict in a_list:
            body += "<tr><td>%s</td></tr>\n" % ("</td><td>".join([sub_dict[key] for key in keys]))
    return "<table>%s\n%s</table>" % (header, body)


def dict_to_html_table(a_dict):
    body = ""
    keys = sorted(a_dict.keys())
    for key in keys:
        body += "<tr><th>%s</th><td>%s</td></tr>\n" % (str(key), str(a_dict[key]))
    return "<table>%s</table>" % body
