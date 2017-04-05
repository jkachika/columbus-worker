import Queue
import collections
import copy
import multiprocessing
import os
import socket
import threading
import time
import traceback
from threading import Thread

import logging
import jsonpickle
import psutil

from colorker.comm import messaging
from colorker.core import EngineProcess
from colorker.core import Target
from colorker.core import FlowStatus
from colorker.core import TargetInput
from colorker.utils import deep_update

logger = logging.getLogger('app')


class TargetScheduler(Thread):
    def __init__(self, container_size, worker):
        """
        :param container_size - size of a container in MB
        """
        super(TargetScheduler, self).__init__()
        self.hostname = socket.getfqdn()
        self.ready_queue = dict()
        self.running_queue = collections.deque()
        self.shelve = collections.deque()
        self.lock = threading.Condition()
        self.manager = multiprocessing.Manager()
        reserved_memory = 4 * 1024 * 1024 * 1024  # 4 GB for running Galileo and Worker
        available_memory = psutil.virtual_memory().available - reserved_memory
        availablity = available_memory // (1024 * 1024)  # MB
        self.vacancy = availablity // container_size
        self.container_size = container_size
        self.max_vacancy = self.vacancy
        self.user_running_flows = dict()
        self.worker = worker
        self.users = set()
        self.terminated = False
        self.__event_queue = Queue.Queue()

    def add_event(self, epid):
        self.__event_queue.put(epid)

    def terminate(self):
        self.terminated = True

    def is_terminated(self):
        return self.terminated

    def awake(self):
        self.lock.acquire()
        self.lock.notifyAll()
        self.lock.release()

    def add_targets(self, pipeline, worker):
        self.lock.acquire()
        if pipeline[messaging.PipelineRequest.USER] not in self.users:
            self.users.add(pipeline[messaging.PipelineRequest.USER])
            self.ready_queue[pipeline[messaging.PipelineRequest.USER]] = dict()
        user_pipelines = self.ready_queue[pipeline[messaging.PipelineRequest.USER]]
        if pipeline[messaging.PipelineRequest.ID] not in user_pipelines:
            user_pipelines[pipeline[messaging.PipelineRequest.ID]] = collections.deque()
        user_queue = user_pipelines[pipeline[messaging.PipelineRequest.ID]]
        for target in pipeline[messaging.PipelineRequest.TARGETS]:
            user_queue.append(EngineProcess(flow_id=pipeline[messaging.PipelineRequest.ID], num_containers=1,
                                            target=Target.from_json(target), status=self.manager.dict(),
                                            container_size=self.container_size, master=self.worker.supervisor,
                                            settings=pipeline[messaging.PipelineRequest.SETTINGS], worker=worker))
        self.lock.notify()
        self.lock.release()

    def __increment_vacancy(self, value=1):
        self.lock.acquire()
        self.vacancy += value
        self.lock.release()

    def __decrement_vacancy(self, value=1):
        self.lock.acquire()
        self.vacancy -= value
        self.lock.release()

    def __remove_from_running_queue(self, process):
        self.lock.acquire()
        self.running_queue.remove(process)
        self.vacancy += process.num_containers
        self.lock.release()

    def __delete_from_user_queue(self, user, flow):
        self.lock.acquire()
        for flow_tuple in self.user_running_flows[user]:
            if flow_tuple[0] == flow:
                self.user_running_flows[user].remove(flow_tuple)
                break
        self.lock.release()

    def __add_to_running_queue(self, process):
        self.lock.acquire()
        self.running_queue.append(process)
        self.lock.release()

    def get_workload(self):
        self.lock.acquire()
        overall_running = len(self.running_queue)
        utilization = 0
        for ep in self.running_queue:
            utilization += ep.num_containers
        overall_waiting = 0
        for user in self.ready_queue:
            count = 0
            for flow in self.ready_queue[user]:
                count += len(self.ready_queue[user][flow])
            overall_waiting += count
        current_shelved = len(self.shelve)
        for ep in self.shelve:
            utilization += ep.num_containers
        utilization = float(utilization) / self.max_vacancy
        total_load = overall_waiting + overall_running + current_shelved
        self.lock.release()
        return messaging.WorkerWorkloadResponse(total=total_load, waiting=overall_waiting, running=overall_running,
                                                shelved=current_shelved, utilization=utilization)

    def get_snapshot(self, acquire):
        if acquire:
            self.lock.acquire()
        overall_running = len(self.running_queue)
        overall_waiting = 0
        user_waiting = {}
        user_waiting_flows = {}
        for user in self.ready_queue:
            count = 0
            for flow in self.ready_queue[user]:
                count += len(self.ready_queue[user][flow])
            user_waiting[user] = count
            user_waiting_flows[user] = len(self.ready_queue[user].keys())
            overall_waiting += count
        user_running = {}
        for user in self.user_running_flows:
            user_stats = dict(num_processes=len(self.user_running_flows[user]))
            cpu_percent, memory_percent, total_threads, total_fds = 0, 0, 0, 0
            for _, pid in self.user_running_flows[user]:
                try:
                    p = psutil.Process(pid)
                    cpu_percent += p.cpu_percent(interval=1.0)
                    memory_percent += p.memory_percent()
                    total_threads += p.num_threads()
                    total_fds += p.num_fds()
                except:
                    pass
            user_stats["cpu_percent"] = cpu_percent
            user_stats["memory_percent"] = memory_percent
            user_stats["total_threads"] = total_threads
            user_stats["total_fds"] = total_fds
            user_running[user] = user_stats

        current_shelved = len(self.shelve)
        utilization = float(self.max_vacancy - self.vacancy) / self.max_vacancy
        total_load = overall_waiting + overall_running + current_shelved
        snapshot = messaging.WorkerSnapshotResponse(
            num_users=len(self.users), container_size=self.container_size, current_vacancy=self.vacancy,
            total_vacancy=self.max_vacancy, overall_waiting=overall_waiting, overall_running=overall_running,
            user_waiting=user_waiting, user_running=user_running, total_workload=total_load,
            current_shelved=current_shelved, utilization=utilization, hostname=self.hostname, system_time=time.time(),
            user_waiting_flows=user_waiting_flows)
        if acquire:
            self.lock.release()
        return snapshot

    def push_snapshot(self, acquire=True):
        try:
            messaging.push(self.worker.supervisor, messaging.Request(
                messaging.RequestType.SNAPSHOT, self.get_snapshot(acquire)))
        except BaseException as e:
            logger.error("Failed to send snapshot to supervisor: %s" % e.message)
            logger.error(traceback.format_exc())

    def run(self):
        last_user = 0
        previous_snapshot_timestamp = time.time()
        while not self.is_terminated():
            try:
                self.lock.acquire()
                while True:
                    if self.vacancy > 0:
                        user_list = sorted(self.users)
                        num_users = len(user_list)
                        # for _ in range(last_user, min(last_user + len(user_list), last_user + self.vacancy)):
                        if num_users > 0:
                            while self.vacancy > 0 and num_users > 0:
                                no_more_targets = True
                                for _ in range(last_user, last_user + self.vacancy):
                                    user = user_list[last_user % num_users]
                                    last_user = (last_user + 1) % num_users
                                    if user not in self.user_running_flows:
                                        self.user_running_flows[user] = list()
                                    running_flows = self.user_running_flows[user]
                                    if user in self.ready_queue:
                                        for flow in self.ready_queue[user].keys():
                                            process_flow = True
                                            for running_flow in running_flows:
                                                if flow == running_flow[0]:
                                                    process_flow = False
                                                    break
                                            if process_flow:
                                                target_process = self.ready_queue[user][flow].popleft()
                                                if len(self.ready_queue[user][flow]) == 0:
                                                    self.ready_queue[user].pop(flow, None)
                                                target_process.send_status()
                                                target_process.start()
                                                logger.info("Target started: pid=%d, target=%s, flow=%s, isAlive=%s" % (
                                                    target_process.pid, target_process.target_name,
                                                    target_process.flow_id, target_process.is_alive()))
                                                self.running_queue.append(target_process)
                                                running_flows.append((flow, target_process.pid))
                                                self.vacancy -= 1
                                                no_more_targets = False
                                                break
                                        if len(running_flows) == 0 and len(self.ready_queue[user].keys()) == 0:
                                            # remove the user from the set and ready queue
                                            self.ready_queue.pop(user, None)
                                            self.users.remove(user)
                                            user_list.remove(user)
                                            num_users -= 1
                                            if last_user > 0:
                                                last_user -= 1
                                            if num_users == 0:
                                                break
                                if no_more_targets:
                                    break
                        if len(self.running_queue) != 0:
                            break
                        self.push_snapshot(acquire=False)
                        logger.info("Scheduler is going to sleep")
                        self.lock.wait()  # Go to sleep until the Supervisor awakes
                        if self.is_terminated():
                            break
                        logger.info("Scheduler is back up and running")
                    else:
                        if len(self.running_queue) != 0:
                            logger.info("No vacancy in the running queue. Will be back when any process terminates")
                            self.lock.wait()
                            logger.info("Recieved awake request. Continuing with the processing")
                            break
                        self.push_snapshot(acquire=False)
                        logger.info("Scheduler is going to sleep")
                        self.lock.wait()  # Go to sleep until the Supervisor awakes
                        if self.is_terminated():
                            break
                        logger.info("Scheduler is back up and running")
            except BaseException as e:
                logger.error("Something went wrong while trying the add targets to the queue: %s" % e.message)
                logger.error(traceback.format_exc())
            finally:
                self.lock.release()

            if self.is_terminated():
                break

            if time.time() - previous_snapshot_timestamp >= 1:
                self.push_snapshot()
                previous_snapshot_timestamp = time.time()

            try:
                unsuccessful_targets, reparable_targets, finished_targets = [], [], []
                for target_process in self.running_queue:
                    if not target_process.is_alive():
                        logger.info("Target finished: pid=%d, target=%s, flow=%s, isAlive=%s" % (
                            target_process.pid, target_process.target_name, target_process.flow_id,
                            target_process.is_alive()))
                        if "error" in target_process.status:
                            if target_process.status["error"] == MemoryError.__name__:
                                if target_process.num_containers >= self.max_vacancy:
                                    history = target_process.target.history
                                    FlowStatus(history=history.id, title='Task Failed',
                                               description='%s failed to execute. Reason: Exceeded max available memory' % (
                                                   history.flow_name),
                                               result='Failed', target_id=target_process.target.element.id,
                                               element='Post-Processing').save(self.worker.supervisor)
                                    target_process.target.history.status = "Failed"
                                    target_process.target.history.save(self.worker.supervisor)
                                    unsuccessful_targets.append(target_process)
                                    logger.info("Target=%s of flow %d failed. Exception=%s Details=%s" % (
                                        target_process.target_name, target_process.flow_id,
                                        target_process.status.get("error", None),
                                        target_process.status.get("details", None)))
                                    continue
                                target_process.target.history.status = "Retry"
                                target_process.target.history.save(self.worker.supervisor)
                                target_process.reserve = target_process.num_containers
                                if 2 * target_process.num_containers > self.max_vacancy:
                                    target_process.reserve = self.max_vacancy - target_process.num_containers
                                reparable_targets.append(target_process)
                            else:
                                # target failed for some other reason. stop processing the workflow
                                history = target_process.target.history
                                FlowStatus(history=history.id, title='Task Unsuccessful',
                                           description='%s failed to execute and was removed. Details: %s' % (
                                               history.flow_name, target_process.status.get("details", None)),
                                           result='Failed', target_id=target_process.target.element.id,
                                           element='Post-Processing').save(self.worker.supervisor)
                                target_process.target.history.status = "Failed"
                                target_process.target.history.save(self.worker.supervisor)
                                unsuccessful_targets.append(target_process)
                                logger.info("Target=%s of flow %d failed. Exception=%s Details=%s" % (
                                    target_process.target_name, target_process.flow_id,
                                    target_process.status.get("error", None),
                                    target_process.status.get("details", None)))
                        else:
                            # target finished smoothly
                            finished_targets.append(target_process)

                if self.is_terminated():
                    break

                if time.time() - previous_snapshot_timestamp >= 1:
                    self.push_snapshot()
                    previous_snapshot_timestamp = time.time()

                for target_process in finished_targets:
                    target_process.target.history.status = "Finished"
                    target_process.target.history.save(self.worker.supervisor)
                    self.__remove_from_running_queue(target_process)
                    user = target_process.target.history.user
                    flow = target_process.flow_id
                    self.__delete_from_user_queue(user, flow)
                    target_output = target_process.status.get("output", None)
                    if target_output is not None:
                        next_target_input = TargetInput.from_json(jsonpickle.decode(target_output))
                        user = target_process.target.history.user
                        flow = target_process.flow_id
                        if flow in self.ready_queue[user]:
                            self.lock.acquire()
                            for next_target_process in self.ready_queue[user][flow]:
                                for element in next_target_input.parents:
                                    if element in next_target_process.target.input.parents and len(
                                            next_target_process.target.input.local_pickles[element]) == 0:
                                        next_target_process.target.input.local_pickles[element].extend(
                                            next_target_input.local_pickles[element])
                                        next_target_process.target.input.global_pickles[element].extend(
                                            next_target_input.global_pickles[element])
                            self.lock.release()
                    else:
                        logger.warning("Target=%s of flow %d did not produce output." % (
                            target_process.target_name, target_process.flow_id))

                if self.is_terminated():
                    break

                for target_process in unsuccessful_targets:
                    # remove all the targets of this pipeline from user's ready queue
                    self.__remove_from_running_queue(target_process)
                    user = target_process.target.history.user
                    flow = target_process.flow_id
                    self.__delete_from_user_queue(user, flow)
                    self.lock.acquire()
                    ignored_processes = self.ready_queue[user].pop(flow, None)
                    self.lock.release()
                    while ignored_processes:
                        ignored_process = ignored_processes.popleft()
                        history = ignored_process.target.history
                        FlowStatus(history=history.id, title='Task Ignored',
                                   description='At least one of the targets failed to execute in the flow %s' % (
                                       history.flow_name),
                                   result='Failed', target_id=ignored_process.target.element.id,
                                   element=ignored_process.target.element.name).save(self.worker.supervisor)
                        ignored_process.target.history.status = "Failed"
                        ignored_process.target.history.save(self.worker.supervisor)

                if self.is_terminated():
                    break

                for target in reparable_targets:
                    # do not claim the containers allocated to the reparable targets
                    self.lock.acquire()
                    self.running_queue.remove(target)
                    self.shelve.append(target)
                    self.lock.release()

                if self.is_terminated():
                    break

                while self.vacancy > 0 and len(self.shelve) > 0:
                    # ft is failed target and nt is the new target in its place
                    self.lock.acquire()
                    ft = self.shelve.popleft()
                    self.lock.release()
                    available_containers = min(ft.reserve, self.vacancy)
                    ft.num_containers += available_containers
                    ft.reserve -= available_containers
                    self.__decrement_vacancy(available_containers)
                    if ft.reserve == 0:
                        logger.info("Changing the number of containers to %d for the target of flow %d" % (
                            ft.num_containers, ft.flow_id))
                        # re run the target with the new number of containers
                        nt = EngineProcess(flow_id=ft.flow_id, num_containers=ft.num_containers, target=ft.target,
                                           container_size=self.container_size, settings=ft.settings,
                                           status=self.manager.dict(), master=self.worker.supervisor, worker=ft.worker)
                        nt.start()
                        self.__add_to_running_queue(nt)
                    else:
                        self.lock.acquire()
                        self.shelve.appendleft(ft)
                        self.lock.release()
                        break
                else:
                    if self.is_terminated():
                        break
            except BaseException as e:
                logger.error("Something went wrong during the processing: %s" % e.message)
                logger.error(traceback.format_exc())

        logger.info("Scheduler is shutting down")
        self.lock.acquire()
        for ep in self.running_queue:
            try:
                logger.info("Terminating engine process:%s" % str(ep))
                p = psutil.Process(ep.pid)
                p.terminate()
            except:
                pass
        self.lock.release()
        return


class WorkerAssistant(Thread):
    def __init__(self, worker):
        super(WorkerAssistant, self).__init__()
        self.worker = worker
        self.clients = Queue.Queue()
        self.__shutdown = False
        self.__worker_address = None

    def set_worker_address(self, address):
        self.__worker_address = address

    def add_client(self, client, address):
        self.clients.put((client, address))

    def is_shutdown(self):
        return self.__shutdown

    def run(self):
        while True:
            client, address = self.clients.get()
            try:
                msg = messaging.recv(client)
                if msg[messaging.Request.TYPE] == messaging.RequestType.PIPELINE:
                    pipeline = msg[messaging.Request.BODY]
                    logger.debug("%s" % pipeline)
                    settings = copy.deepcopy(self.worker.global_settings)
                    settings = deep_update(settings, pipeline.get(messaging.PipelineRequest.SETTINGS, {}))
                    pipeline[messaging.PipelineRequest.SETTINGS] = settings
                    self.worker.scheduler.add_targets(pipeline, self.__worker_address)
                elif msg[messaging.Request.TYPE] == messaging.RequestType.WORKLOAD:
                    messaging.send(client, self.worker.scheduler.get_workload())
                elif msg[messaging.Request.TYPE] == messaging.RequestType.SNAPSHOT:
                    messaging.send(client, self.worker.scheduler.get_snapshot(acquire=True))
                elif msg[messaging.Request.TYPE] == messaging.RequestType.FILE:
                    file_request = msg[messaging.Request.BODY]
                    # filesystem = file_request[messaging.WorkerFileRequest.FILESYSTEM]
                    file_paths = file_request[messaging.WorkerFileRequest.FILE_PATHS]
                    contents = []
                    for file_path in file_paths:
                        try:
                            with open(file_path, 'r') as result_file:
                                contents.extend(result_file.read().splitlines())
                            # if "/%s/" % filesystem not in file_path:
                            #     os.remove(file_path)
                        except:
                            logger.error(traceback.format_exc())
                    messaging.send(client, contents)
                    del contents
                elif msg[messaging.Request.TYPE] == messaging.RequestType.TERMINATION:
                    self.worker.terminate()
                    messaging.push(self.__worker_address, messaging.Request(messaging.RequestType.TERMINATION))
                    break
                elif msg[messaging.Request.TYPE] == messaging.RequestType.FINISHED:
                    self.worker.scheduler.awake()
            except:
                logger.error("Something went wrong while processing the request from the client")
                logger.error(traceback.format_exc())
            finally:
                try:
                    client.close()
                except:
                    pass
        logger.info("Assistant shutdown complete")
        return


class Worker(Thread):
    def __init__(self, master):
        """
        Instantiates a worker thread. A start() call on the resulting object should be made to run the worker
        :param master: (ip-address, port-number) tuple
        """
        super(Worker, self).__init__()
        self.supervisor = master
        self.container_size = 0
        self.global_settings = None
        self.scheduler = None
        self.__assistant = WorkerAssistant(self)
        self.terminated = False

    def is_terminated(self):
        return self.terminated

    def terminate(self):
        self.terminated = True

    def run(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('', 0))
        server_port = server.getsockname()[1]
        logger.info("Worker started on port %d" % server_port)
        logger.info("informing assistant on localhost(%s)" % server.getsockname()[0])
        self.__assistant.set_worker_address((str(server.getsockname()[0]), server_port))
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        logger.info("contacting supervisor on " + str(socket.getfqdn(self.supervisor[0])))
        client.connect(self.supervisor)
        config_request = messaging.Request(messaging.RequestType.CONFIG)
        messaging.send(client, config_request)
        response = messaging.recv(client)
        # 1 GB default container size if none was returned, mentioned in MB = 1024
        self.container_size = response.get(messaging.SupervisorConfigResponse.CONTAINER_SIZE, 1024)
        self.global_settings = response.get(messaging.SupervisorConfigResponse.GLOBAL_SETTINGS, {})
        logger.info('starting the assistant')
        self.__assistant.start()
        logger.debug('supervisor config response - %s' % response)
        self.scheduler = TargetScheduler(container_size=self.container_size, worker=self)
        config_request = messaging.Request(messaging.RequestType.CONFIG, messaging.WorkerConfigResponse(
            port_num=server_port, num_cores=psutil.cpu_count(), process_id=os.getpid(),
            num_slots=self.scheduler.max_vacancy, available_mem=psutil.virtual_memory().available,
            max_memory=psutil.virtual_memory().total))
        logger.info("sending worker configuration to supervisor")
        messaging.send(client, config_request)
        client.close()
        logger.info('starting the scheduler')
        self.scheduler.start()
        server.listen(5)
        while not self.is_terminated():
            client, address = server.accept()
            if not self.is_terminated():
                self.__assistant.add_client(client, address)
        logger.info("Worker is shutting down")
        self.scheduler.awake()
        self.scheduler.terminate()
        if self.scheduler.isAlive():
            logger.info("Awaiting scheduler termination")
            self.scheduler.join()
            logger.info("Scheduler terminated")
        if self.__assistant.isAlive():
            logger.info("Awaiting assistant termination")
            self.__assistant.join()
            logger.info("Assistant terminated")
        logger.info("Shutdown complete")
