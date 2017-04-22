#!/usr/bin/python
#
# Author: Johnson Kachikaran (johnsoncharles26@gmail.com)
# Date: 19th May 2016

"""
Includes functions that facilitate sending an email to the requested people
"""

import logging
import threading
import traceback
import re

from colorker.comm import messaging

logger = logging.getLogger('worker')


def send_mail(receivers, subject, message, html=None):
    """
    Sends an email to the recipients. Must be called from an EngineThread. This method will not raise any exception
    if it fails to send a message to the recipients.

    :param list(str) receivers: list of recipient email addresses
    :param str subject: subject of the email
    :param str message: plain text message
    :param str html: HTML message
    """
    if not isinstance(receivers, list):
        raise ValueError('Invalid recipients. Must be a list of email addresses.')
    try:
        if not subject or not message:
            raise ValueError('subject and message body are required to send the email')
        sender = threading.current_thread().username
        master = threading.current_thread().master
        if html is None:
            html = re.sub("\r?\n", "<br/>", message)
        request = messaging.Request(messaging.RequestType.EMAIL, messaging.WorkerEmailRequest(
            sender=sender, receivers=receivers, subject=subject, plain=message, html=html))
        messaging.push(master, request)
    except Exception as e:
        logger.error(e.message)
        logger.error(traceback.format_exc())
