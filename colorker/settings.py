import logging
import logging.handlers


class CREDENTIALS:
    SERVER = "credentials.server"
    CLIENT = "credentials.client"


class STORAGE:
    class TEMPORARY:
        CLOUD = "storage.temporary.cloud"
        LOCAL = "storage.temporary.local"

    class PERSISTENT:
        CLOUD = "storage.persistent.cloud"
        LOCAL = "storage.persistent.local"

    class READABLE:
        CLOUD = "storage.readable.cloud"


def configure_logging(logger_name, logfile_path, level=logging.INFO):
    l = logging.getLogger(logger_name)

    default_formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(process)d/%(threadName)s] [%(name)s] [%(funcName)s():%(lineno)s] %(message)s",
        "%d/%m/%Y %H:%M:%S")

    file_handler = logging.handlers.RotatingFileHandler(logfile_path, maxBytes=1024 * 1024 * 10, backupCount=300,
                                                        encoding='utf-8')
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    file_handler.setFormatter(default_formatter)
    console_handler.setFormatter(default_formatter)

    l.setLevel(level)
    l.addHandler(file_handler)
    l.addHandler(console_handler)
    l.propagate = False
