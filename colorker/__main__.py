import os
import socket
import sys

from colorker import settings
from app import Worker
from multiprocessing import freeze_support

if len(sys.argv) < 3:
    print ("Usage: python -m colorker <master-ip-address> <master-port>"
           " [<optional-log-file-path(default=./hostname.log)>]")
else:
    if os.name == 'nt':
        freeze_support()
    args = sys.argv
    master_ip = args[1]
    master_port = int(args[2])
    log_path = "./%s.log" % (socket.gethostname())
    if len(args) > 3:
        log_path = args[3]
    settings.configure_logging(logger_name='app', logfile_path="%s-app.log" % log_path[:-4])
    settings.configure_logging(logger_name='worker', logfile_path="%s-worker.log" % log_path[:-4])
    worker = Worker(master=(master_ip, master_port))
    worker.start()
    worker.join()
