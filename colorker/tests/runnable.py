from colorker import settings
from colorker.app import Worker
from multiprocessing import freeze_support

if __name__ == '__main__':
    freeze_support()
    log_path = "../../colorker.log"
    settings.configure_logging(logfile_path=log_path)
    worker = Worker(master=('127.0.0.1', 56789))
    worker.start()
    worker.join()
