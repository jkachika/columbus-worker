"""
Includes utility functions that can be used in the code composed in Components and Combiners of the Columbus platform
"""

import collections
import math
import time
from datetime import datetime as dt

import numpy


def current_time_millis():
    """
    Gets the current time in milliseconds

    :rtype: int
    :return: current time in milliseconds
    """
    return int(round(time.time() * 1000))


# noinspection PyBroadException
def caught(try_function, *args):
    """
    Tries a function and checks if it throws an exception.

    :param Callable try_function: callable object representing the function that must be tried
    :param list args: arguments to pass to the callable function

    :rtype: bool
    :return: True if an exception was caught, False otherwise
    """
    try:
        try_function(*args)
        return False
    except BaseException:
        return True


def is_number(s):
    """
    Checks if the argument is a number

    :param str s: Any string

    :rtype: bool
    :return: True if the string is a number, False otherwise
    """
    return False if caught(float, s) else True


# finds the mean of a feature collection for a given property
def mean(prop, ftc):
    """
    Finds the mean of a property in the given feature collection. NaN values are treated as zero.

    :param str prop: name of the property in the feature collection
    :param geojson.FeatureCollection ftc:  the feature collection containing that property

    :return: mean value of the property
    :rtype: float
    """
    features = ftc['features']
    result = [(float(feature['properties'][prop]) if is_number(feature['properties'][prop]) and not math.isnan(
        float(feature['properties'][prop])) else 0.0) for feature in features]
    return numpy.mean(numpy.array(result))


# finds the standard deviation of a feature collection for a given property
def std(prop, ftc):
    """
    Finds the standard deviation of a property in the given feature collection. NaN values are treated as zero.

    :param str prop: name of the property in the feature collection
    :param geojson.FeatureCollection ftc:  the feature collection containing that property

    :return: standard deviation value of the property
    :rtype: float
    """
    features = ftc['features']
    result = [(float(feature['properties'][prop]) if is_number(feature['properties'][prop]) and not math.isnan(
        float(feature['properties'][prop])) else 0.0) for feature in features]
    return numpy.std(numpy.array(result))


def json_serial(obj):
    """
    JSON serializer for objects not serializable by default json code.
    TODO - Add implementation for other types that are not serializable

    :param object obj: The object that needs to be serialized

    :return: json serialization of the given object
    :rtype: str
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
    """
    Updates a nested dictionary or similar mapping. Modifies ``source`` in place with the key-value pairs in overrides

    :param dict source: a dictionary that needs to be updated
    :param dict overrides: a dictionary that provides the new keys and values

    :rtype: dict
    :return: updated source dictionary
    """
    for key, value in overrides.iteritems():
        if isinstance(value, collections.Mapping) and value:
            returned = deep_update(source.get(key, {}), value)
            source[key] = returned
        else:
            source[key] = overrides[key]
    return source


def lists_to_html_table(a_list):
    """
    Converts a list of lists to a HTML table. First list becomes the header of the table.
    Useful while sending email from the code

    :param list(list) a_list: values in the form of list of lists

    :return: HTML table representation corresponding to the values in the lists
    :rtype: str
    """
    header = "<tr><th>%s</th></tr>" % ("</th><th>".join(a_list[0]))
    body = ""
    if len(a_list) > 1:
        for sub_list in a_list[1:]:
            body += "<tr><td>%s</td></tr>\n" % ("</td><td>".join(sub_list))
    return "<table>%s\n%s</table>" % (header, body)


def dicts_to_html_table(a_list):
    """
    Converts a list of dictionaries to a HTML table. Keys become the header of the table.
    Useful while sending email from the code

    :param list(dict) a_list: values in the form of list of dictionaries

    :return: HTML table representation corresponding to the values in the lists
    :rtype: str
    """
    keys = sorted(a_list[0].keys())
    header = "<tr><th>%s</th></tr>" % ("</th><th>".join(keys))
    body = ""
    if len(a_list) > 1:
        for sub_dict in a_list:
            body += "<tr><td>%s</td></tr>\n" % ("</td><td>".join([sub_dict[key] for key in keys]))
    return "<table>%s\n%s</table>" % (header, body)


def dict_to_html_table(a_dict):
    """
    Converts a dictionary to a HTML table. Keys become the header of the table.
    Useful while sending email from the code

    :param dict a_dict: key value pairs in the form of a dictionary

    :return: HTML table representation corresponding to the values in the dictionary
    :rtype: str
    """
    body = ""
    keys = sorted(a_dict.keys())
    for key in keys:
        body += "<tr><th>%s</th><td>%s</td></tr>\n" % (str(key), str(a_dict[key]))
    return "<table>%s</table>" % body
