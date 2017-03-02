import jsonpickle
import socket

MESSAGE_OPEN = "<columbus::message>"
MESSAGE_CLOSE = "<\columbus::message>"
BUFFER_SIZE = 16 * 1024  # 16 KB


def push(server_address, obj, encode=True):
    message = jsonpickle.encode(obj, unpicklable=False) if encode else obj
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(server_address)
    sock.sendall(MESSAGE_OPEN + str(message) + MESSAGE_CLOSE)
    sock.close()


def send(sock, obj, encode=True):
    message = jsonpickle.encode(obj, unpicklable=False) if encode else obj
    sock.sendall(MESSAGE_OPEN + str(message) + MESSAGE_CLOSE)


def recv(sock, decode=True):
    response = ''
    data = sock.recv(BUFFER_SIZE)
    while True:
        response += str(data)
        if MESSAGE_CLOSE in response:
            break
        data = sock.recv(BUFFER_SIZE)
    message = response[len(MESSAGE_OPEN):-len(MESSAGE_CLOSE)]
    return jsonpickle.decode(message) if decode else message


def sendrecv(sock, obj, encode=True, decode=True):
    send(sock=sock, obj=obj, encode=encode)
    response = recv(sock=sock, decode=decode)
    sock.close()
    return response


class RequestType:
    WORKLOAD = "workload"
    PIPELINE = "pipeline"
    CONFIG = "config"
    UPDATE = "update"
    SNAPSHOT = "snapshot"
    FILE = "file"
    EMAIL = "email"
    FINISHED = "finished"
    TERMINATION = "termination"


class PipelineRequest:
    ID = "id"
    USER = "user"
    TARGETS = "targets"
    SETTINGS = "settings"

    def __init__(self, id, user, targets, settings):
        assert isinstance(targets, list)
        assert isinstance(settings, dict)
        self.id = id
        self.user = user
        self.targets = targets
        self.settings = settings


class ModelUpdateRequest:
    MODEL = "model"
    UPDATE = "update"

    def __init__(self, model, update):
        self.model = model
        self.update = update


class WorkerWorkloadResponse:
    UTILIZATION = "utilization"
    TOTAL = "total"
    RUNNING = "running"
    WAITING = "waiting"
    SHELVED = "shelved"

    def __init__(self, total, running, waiting, shelved, utilization):
        self.total = total
        self.running = running
        self.waiting = waiting
        self.shelved = shelved
        self.utilization = utilization


class WorkerSnapshotResponse:
    HOSTNAME = "hostname"
    SYSTEM_TIME = "system_time"
    NUM_USERS = "num_users"
    UTILIZATION = "utilization"
    CONTAINER_SIZE = "container_size"
    CURRENT_VACANCY = "current_vacancy"
    TOTAL_VACANCY = "total_vacancy"
    OVERALL_WAITING = "overall_waiting"
    OVERALL_RUNNING = "overall_running"
    USER_WAITING = "user_waiting"
    USER_RUNNING = "user_running"
    TOTAL_WORKLOAD = "total_load"
    CURRENT_SHELVED = "current_shelve"
    USER_WR_RATIO = "wr_ratio"

    def __init__(self, num_users, container_size, current_vacancy, total_vacancy, overall_waiting, overall_running,
                 user_waiting, user_running, total_workload, current_shelved, utilization, hostname, system_time,
                 user_waiting_flows):
        self.num_users = num_users
        self.container_size = container_size
        self.current_vacancy = current_vacancy
        self.total_vacancy = total_vacancy
        self.overall_waiting = overall_waiting
        self.overall_running = overall_running
        self.user_waiting = user_waiting
        self.user_running = user_running
        self.total_load = total_workload
        self.current_shelve = current_shelved
        self.utilization = utilization
        self.hostname = hostname
        self.system_time = system_time
        self.wr_ratio = {}
        for user in user_waiting_flows:
            num_waiting = user_waiting_flows[user]
            numerator = num_waiting - 1 if num_waiting >= 1 else 0
            if user in self.user_running:
                num_running = self.user_running[user]['num_processes']
                self.wr_ratio[user] = numerator if num_running == 0 else float(num_waiting)/num_running
            self.wr_ratio[user] = numerator


class WorkerConfigResponse:
    PORT_NUMBER = "port_number"
    LOGICAL_CORES = "logical_cores"
    NUM_SLOTS = "slots"
    AVAILABLE_MEMORY = "available_memory"
    TOTAL_MEMORY = "total_memory"
    PROCESS_ID = "process_id"

    def __init__(self, port_num, num_cores, num_slots, available_mem, max_memory, process_id):
        self.port_number = port_num
        self.logical_cores = num_cores
        self.slots = num_slots
        self.available_memory = available_mem
        self.total_memory = max_memory
        self.process_id = process_id


class SupervisorConfigResponse:
    CONTAINER_SIZE = "container_size"
    GLOBAL_SETTINGS = "global_settings"

    def __init__(self, container_size, global_settings):
        self.container_size = container_size
        self.global_settings = global_settings


class TargetFinishedRequest:
    PROCESS_ID = "pid"

    def __init__(self, pid):
        self.pid = pid


class WorkerFileRequest:
    FILESYSTEM = "filesystem"
    FILE_PATHS = "file_paths"

    def __init__(self, filesystem, file_paths):
        self.filesystem = filesystem
        self.file_paths = file_paths


class WorkerEmailRequest:
    FROM = "sender"
    TO = "receivers"
    SUBJECT = "subject"
    HTML = "html"
    PLAIN = "plain"

    def __init__(self, sender, receivers, subject, html, plain):
        self.sender = sender
        self.receivers = receivers
        self.subject = subject
        self.html = html
        self.plain = plain


class Request:
    TYPE = "type"
    BODY = "body"

    def __init__(self, request_type, request_body=None):
        self.type = request_type
        self.body = request_body
