import logging
import threading
import traceback
import re

from colorker.comm import messaging

logger = logging.getLogger('worker')


def send_mail(receivers, subject, message, html=None):
    """
    Sends an email to the recipients. Must be called by an EngineThread.
    :param receivers: list of recipient email addresses
    :param subject: subject of the email
    :param message: plain text message
    :param html: HTML message
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
