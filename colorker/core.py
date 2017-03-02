import cPickle as pickle
import json
import logging
import multiprocessing
import os
import pkgutil
import re
import shutil
import socket
import threading
import time
import traceback
from datetime import datetime

import jsonpickle
from geojson import FeatureCollection, Feature
from pytz import timezone

from colorker.comm import messaging
from colorker.security import ServiceAccount
from colorker.settings import STORAGE
from colorker.utils import json_serial, caught
from service import bigquery
from service import drive
from service import fusiontables
from service import gee

logger = logging.getLogger('worker')
hostname = socket.getfqdn()
resource_loader = pkgutil.find_loader("resource")


def synchronized(func):
    """
    a decorator to make the functions thread-safe
    :param func: function object to which this decorator is added
    :return: returns a synchronized function
    """
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)

    return synced_func


@synchronized
def create_and_copy(src, dst):
    dstdir = os.path.dirname(dst)
    if not os.path.exists(dstdir):
        os.makedirs(dstdir)  # create all directories, raise an error if it already exists
    shutil.copy2(src, dstdir)


class Component(object):
    def __init__(self, id, name, code, type, visualizer=False, root=False, parties=None):
        self.id = id  # id of the component
        self.name = name  # name of the component
        self.sys_parents = []
        self.sys_combiners = []
        self.sys_output = None
        self.sys_code = code
        self.type = type
        self.sorted_key = None
        self.sorted_dir = None
        self.is_root = root
        self.visualizer = visualizer
        self.parties = parties
        self.ftkey = None
        self.csv = None
        self.ftcindex = 0

    def serialize(self, directory, filename, user_settings=None):
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(directory + '/' + filename, 'wb') as handle:
            pickle.dump(self, handle)
        if user_settings is None:
            user_settings = threading.current_thread().settings
        fqfn = "%s/%s" % (directory, filename)
        return gee.upload_object(bucket=user_settings.get(STORAGE.PERSISTENT.CLOUD, None),
                                 filename=fqfn, readers=[], owners=[], user_settings=user_settings,
                                 access=ServiceAccount.GCS)

    @staticmethod
    def from_json(json_dict):
        component = Component(id=json_dict['id'], name=json_dict['name'],
                              code=json_dict['sys_code'], type=json_dict['type'])
        component.__dict__.update(json_dict)
        return component

    def to_json(self):
        return json.dumps(self, default=lambda o: json_serial(o), sort_keys=True)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.id == self.id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return 'component-' + str(self.id)

    def __repr__(self):
        return 'cid-' + str(self.id)


class Combiner(object):
    def __init__(self, id, name, flow_id, flow_name, code, type, is_static=False, start=None, end=None,
                 visualizer=False, parties=None):
        self.id = id
        self.name = name
        self.flow_id = flow_id
        self.flow_name = flow_name
        self.sys_code = code
        self.static = is_static
        self.start = start
        self.end = end
        self.type = type
        self.visualizer = visualizer
        self.parties = parties
        self.sys_output = None
        self.sorted_key = None
        self.sorted_dir = None
        self.ftkey = None
        self.csv = None
        self.ftcindex = 0

    def serialize(self, directory, filename, user_settings=None):
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(directory + '/' + filename, 'wb') as handle:
            pickle.dump(self, handle)
        if user_settings is None:
            user_settings = threading.current_thread().settings
        fqfn = "%s/%s" % (directory, filename)
        return gee.upload_object(bucket=user_settings.get(STORAGE.PERSISTENT.CLOUD, None),
                                 filename=fqfn, readers=[], owners=[], user_settings=user_settings,
                                 access=ServiceAccount.GCS)

    @staticmethod
    def from_json(json_dict):
        combiner = Combiner(id=json_dict['id'], name=json_dict['name'], flow_id=json_dict['flow_id'],
                            flow_name=json_dict['flow_name'], code=json_dict['sys_code'], type=json_dict['type'])
        combiner.__dict__.update(json_dict)
        return combiner

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.id == self.id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return 'combiner-' + str(self.id)

    def __repr__(self):
        return 'mid-' + str(self.id)


class EngineResource(object):
    def __init__(self, source, details, data):
        self.source = source
        self.details = details
        self.sys_output = data
        self.type = 'csv'
        self.sorted_key = None
        self.sorted_dir = None
        self.csv = None
        self.ftcindex = 0

    def serialize(self, directory, filename, user_settings=None):
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(directory + '/' + filename, 'wb') as handle:
            pickle.dump(self, handle)
        if user_settings is None:
            user_settings = threading.current_thread().settings
        fqfn = "%s/%s" % (directory, filename)
        return gee.upload_object(bucket=user_settings.get(STORAGE.PERSISTENT.CLOUD, None),
                                 filename=fqfn, readers=[], owners=[], user_settings=user_settings,
                                 access=ServiceAccount.GCS)

    @staticmethod
    def load(local_pickle, global_pickle, user_settings=None):
        try:
            with open(local_pickle, 'rb') as handle:
                element = pickle.load(handle)
        except:
            if user_settings is None:
                user_settings = threading.current_thread().settings
            names = global_pickle.split('#')
            gee.download_object(bucket=names[0], filename=names[1], out_file=local_pickle,
                                user_settings=user_settings, access=ServiceAccount.GCS)
            with open(local_pickle, 'rb') as handle:
                element = pickle.load(handle)
        return element


class TargetHistory:
    HISTORY_ID = "id"
    USERNAME = "user"
    WORKFLOW_ID = "flow"
    TARGET_ID = "target"
    WORKFLOW_NAME = "flow_name"
    STATUS = "status"
    TIMEZONE = "timezone"
    USER_EMAIL = "admin"

    def __init__(self, history_id, user_id, user_email, flow_id, flow_name, target_id, status, timezone):
        self.id = history_id
        self.user = user_id
        self.flow = flow_id
        self.target = target_id
        self.flow_name = flow_name
        self.status = status
        self.timezone = timezone
        self.admin = user_email  # needed for fusion table assignments

    @staticmethod
    def from_json(json_dict):
        target_history = TargetHistory(history_id=json_dict['id'], user_id=json_dict['user'],
                                       user_email=json_dict['admin'], flow_id=json_dict['flow'],
                                       flow_name=json_dict['flow_name'], target_id=json_dict['target'],
                                       status=json_dict['status'], timezone=json_dict['timezone'])
        return target_history

    def save(self, master):
        try:
            update = messaging.Request(messaging.RequestType.UPDATE, messaging.ModelUpdateRequest(
                model=self.__class__.__name__, update=self))
            messaging.push(master, update)
        except Exception as e:
            logger.error(traceback.format_exc())
            raise type(e)("failed to communicate the update to master - %s" % e.message)


class FlowStatus:
    HISTORY_ID = "history_id"
    TARGET_ID = "target_id"
    TITLE = "title"
    DESCRIPTION = "description"
    RESULT = "result"
    ELEMENT = "element"
    LOCAL_PICKLE = "local_pickle"
    GLOBAL_PICKLE = "global_pickle"
    PICKLE_SIZE = "pickle_size"
    OUTPUT_TYPE = "type"
    FUSION_TABLE_KEYS = "ftkey"
    REFERENCE = "ref"

    def __init__(self, history, title, description, result, target_id, element, local_pickle=None,
                 global_pickle=None, ref=None, type=None, ftkey=None, pickle_size=None):
        self.history_id = history
        self.target_id = target_id
        self.title = title
        self.description = description
        self.result = result
        self.element = element
        self.local_pickle = local_pickle
        self.global_pickle = global_pickle
        self.pickle_size = pickle_size
        self.type = type
        self.ftkey = ftkey
        self.ref = ref

    @staticmethod
    def from_json(json_dict):
        flow_status = FlowStatus(history=json_dict['history_id'], title=json_dict['title'],
                                 description=json_dict['description'], result=json_dict['result'],
                                 target_id=json_dict['target_id'], element=json_dict['element'])
        flow_status.__dict__.update(json_dict)
        return flow_status

    def save(self, master):
        try:
            update = messaging.Request(messaging.RequestType.UPDATE, messaging.ModelUpdateRequest(
                model=self.__class__.__name__, update=self))
            messaging.push(master, update)
        except Exception as e:
            logger.error(traceback.format_exc())
            raise type(e)("failed to communicate the update to master - %s" % e.message)


class Query:
    def __init__(self, source, identifier, description):
        self.source = source
        self.identifier = identifier
        self.desc = description
        self.spatial = None
        self.temporal = None
        self.feature = self.op = self.primitive = self.value = None
        self.constraint = None
        self.results = None

    def set_spatial_property(self, space):
        self.spatial = space

    def set_temporal_property(self, time):
        self.temporal = time

    def set_attribute_filter(self, feature, primitive, op, value):
        self.feature = feature
        self.primitive = primitive
        self.op = op
        self.value = value

    def set_constraint(self, constraint):
        self.constraint = constraint

    def set_results(self, results):
        self.results = results

    @staticmethod
    def from_json(json_dict):
        if json_dict is None:
            return None
        query = Query(source=json_dict['source'], identifier=json_dict['identifier'], description=json_dict['desc'])
        query.__dict__.update(json_dict)
        return query


class TargetInput:
    def __init__(self, parents, local_pickles, global_pickles):
        logger.debug("parents=%s" % parents)
        logger.debug("local_pickles=%s" % local_pickles)
        logger.debug("global_pickles=%s" % global_pickles)
        assert isinstance(parents, list)
        if not all(type(parent) is str for parent in parents):
            raise ValueError('parents must be a list of strings denoting element type and their id separated by -')
        assert isinstance(local_pickles, dict)
        for parent in parents:
            if parent not in local_pickles or not isinstance(local_pickles[parent], list) or not all(
                            type(pickle_path) is str for pickle_path in local_pickles[parent]):
                raise ValueError(('local_pickles must be a dictionary of the members in parents'
                                  ' and values must be a list of pickle paths for that parent'))
        assert isinstance(global_pickles, dict)
        for parent in parents:
            if parent not in global_pickles or not isinstance(global_pickles[parent], list) or not all(
                            type(pickle_path) is str for pickle_path in global_pickles[parent]):
                raise ValueError(('global_pickles must be a dictionary of the members in parents'
                                  ' and values must be a list of pickle paths for that parent'))
        self.parents = parents
        self.local_pickles = local_pickles
        self.global_pickles = global_pickles

    @staticmethod
    def from_json(json_dict):
        if json_dict is None:
            return None
        target_input = TargetInput(parents=json_dict['parents'], local_pickles=json_dict['local_pickles'],
                                   global_pickles=json_dict['global_pickles'])
        return target_input


class Target:
    def __init__(self, name, query, target_input, target_type, element, history):
        assert query is None or isinstance(query, Query)
        assert target_input is None or isinstance(target_input, TargetInput)
        assert query is not None or target_input is not None, "Either query or input is required for a target"
        elements = {Component.__name__: Component, Combiner.__name__: Combiner}
        assert target_type in elements
        assert isinstance(element, elements[target_type])
        assert isinstance(history, TargetHistory)
        self.name = name
        self.query = query
        self.input = target_input
        self.type = target_type
        self.element = element
        self.history = history

    @staticmethod
    def from_json(json_dict):
        elements = {Component.__name__: Component, Combiner.__name__: Combiner}
        target = Target(name=json_dict['name'], query=Query.from_json(json_dict.get('query', None)),
                        target_input=TargetInput.from_json(json_dict.get('input', None)),
                        target_type=json_dict['type'],
                        element=elements[json_dict['type']].from_json(json_dict['element']),
                        history=TargetHistory.from_json(json_dict['history']))
        return target


class EngineThread(threading.Thread):
    def __init__(self, settings, master, history, element, query, input):
        super(EngineThread, self).__init__()
        self.username = history.user
        self.settings = settings
        self.master = master
        self.history = history
        self.element = element
        self.query = query
        self.input = input
        self.active = True
        self.output = None
        self.__error = None
        self.__trace = None

    def is_successful(self):
        return self.__error is None

    def is_running(self):
        return self.active

    def get_error_trace(self):
        return self.__error, self.__trace

    def execute_code(self):
        __input__ = self.element.sys_input
        scope = dict(locals(), **globals())
        exec (compile(self.element.sys_code, self.element.name + '.py', 'exec'), scope)
        return scope.get("__output__", __input__)

    def verify_output(self):
        try:
            FlowStatus(history=self.history.id, title='Verifying Output',
                       description='checking the output of ' + self.element.name + ' for its output type.',
                       result='Pending', target_id=self.element.id, element=self.element.name).save(self.master)
            __output__ = self.element.sys_output
            if self.element.type == 'ftc':
                if not isinstance(__output__, FeatureCollection):
                    raise TypeError("Output type mismatch: expected a geojson feature collection")
            elif self.element.type == 'mftc':
                if not isinstance(__output__, list):
                    raise TypeError("Output type mismatch: expected a list of geojson feature collections")
                for ftc in __output__:
                    if not isinstance(ftc, FeatureCollection):
                        raise TypeError("Output type mismatch: expected a list of geojson feature collections")
            elif self.element.type == 'csv':
                if not isinstance(__output__, list):
                    raise TypeError("Output type mismatch: expected a list of dictionaries")
                for row in __output__:
                    if not isinstance(row, dict):
                        raise TypeError("Output type mismatch: expected a list of dictionaries")
            elif self.element.type == 'ft':
                if not isinstance(__output__, Feature):
                    raise TypeError("Output type mismatch: expected a geojson feature")

            FlowStatus(history=self.history.id, title='Verification Completed',
                       description=self.element.name + '\'s output matches with it\'s type.',
                       result='Success', target_id=self.element.id, element=self.element.name).save(self.master)

        except BaseException as e:
            if isinstance(e, MemoryError):
                raise e
            logger.error(traceback.format_exc())
            FlowStatus(history=self.history.id, title='Verification Failed',
                       description='The output of ' + self.element.name + ' does not match it\'s output type.',
                       result='Failed', target_id=self.element.id, element=self.element.name).save(self.master)
            element_type = self.element.__class__.__name__
            raise type(e)(str(element_type) + ' <b>' + self.element.name + '</b> Failed - ' + e.message)

    def execute_element(self):
        element_type = self.element.__class__.__name__
        try:
            FlowStatus(title='Executing ' + str(element_type),
                       description='Executing the code for ' + str(
                           element_type).lower() + ' -  ' + self.element.name + '.', result='Pending',
                       target_id=self.element.id, element=self.element.name, history=self.history.id).save(self.master)
            self.element.sys_output = self.execute_code()
            self.element.sys_input = None  # we don't need the input to be pickled
            self.verify_output()
            filekey = "c" if isinstance(self.element, Component) else "m"
            logger.info("Is element " + str(self.element.name) + " having type " + str(
                self.element.type) + " visualizer? - " + str(self.element.visualizer))
            if self.element.visualizer:
                admin = self.history.admin
                desc = 'Columbus: ' + str(self.element.name)
                if self.element.type == 'ftc':
                    ft_name = re.sub('[^a-zA-Z0-9]', '-', self.element.name)
                    ft_name = ft_name + "-h" + str(self.history.id) + filekey + str(self.element.id)
                    ftkey = fusiontables.create_ft_from_ftc(name=ft_name, description=desc,
                                                            ftc=self.element.sys_output, parties=self.element.parties,
                                                            admin=admin)
                    self.element.ftkey = ftkey
                elif self.element.type == 'mftc':
                    __output__ = self.element.sys_output
                    ftkeys = []
                    for index, ftc in enumerate(__output__):
                        ft_name = re.sub('[^a-zA-Z0-9]', '-', self.element.name)
                        ft_name += "-h" + str(self.history.id) + filekey + str(
                            self.element.id) + "-ftc-" + ftc.get('id', str(index))
                        ftkey = fusiontables.create_ft_from_ftc(name=ft_name, description=desc, ftc=ftc,
                                                                parties=self.element.parties, admin=admin)
                        ftkeys.append(ftkey)
                    self.element.ftkey = ",".join(ftkeys)
            directory = "%s%s/pickles/targets/h%d" % (EngineThread.get_engine_setting(
                STORAGE.PERSISTENT.LOCAL), self.username, self.history.id)
            fileref = ("component-" if isinstance(self.element, Component) else "combiner-") + str(self.element.id)
            filename = "h" + str(self.history.id) + filekey + str(self.element.id) + ".pickle"
            pickle_response = self.element.serialize(directory=directory, filename=filename)
            pickle_size = pickle_response['size']
            pickle_name = "%s/%s" % (directory, filename)
            gcs_pickle_name = "%s#%s" % (pickle_response['bucket'], pickle_response['name'])
            FlowStatus(history=self.history.id, title=str(element_type) + ' Executed',
                       description=self.element.name + ' executed successfully.', result='Success', ref=fileref,
                       target_id=self.element.id, element=self.element.name, local_pickle=pickle_name,
                       global_pickle=gcs_pickle_name, type=self.element.type,
                       ftkey=self.element.ftkey, pickle_size=pickle_size).save(self.master)
            parent_keys, local_pickles, global_pickles = [], {}, {}
            parent_keys.append(str(self.element.__class__.__name__).lower() + "-" + str(self.element.id))
            local_pickles[parent_keys[-1]] = [str(pickle_name)]
            global_pickles[parent_keys[-1]] = [str(gcs_pickle_name)]
            self.output = TargetInput(parents=parent_keys, local_pickles=local_pickles, global_pickles=global_pickles)
        except BaseException as e:
            if isinstance(e, MemoryError):
                raise e
            trace = traceback.format_exc()
            exc_trace = trace
            logger.error(trace)
            logger.error(e.message)
            if "exec (compile(__code__, __name__ + '.py', 'exec'), scope)" in str(trace):
                sub_trace = str(trace).split("exec (compile(__code__, __name__ + '.py', 'exec'), scope)")[-1]
                sub_trace = sub_trace.replace('<module>', 'module').replace('\n', '<br/>')
                exc_trace = e.message + '<br/><b>Trace:</b>' + sub_trace.strip()
            FlowStatus(history=self.history.id, title='Execution Failed',
                       description=self.element.name + ' failed to execute. Reason - ' + e.message,
                       result='Failed', target_id=self.element.id, element=self.element.name).save(self.master)
            raise type(e)(str(element_type) + ' <b>' + self.element.name + '</b> Failed - ' + exc_trace)

    def fetch_raw_data(self):
        source = self.query.source
        identifier = self.query.identifier
        if source == 'bigquery':
            feature = self.query.feature
            primitive, op, value = self.query.primitive, self.query.op, self.query.value
            constraint = self.query.constraint
            if int(primitive) == 9:  # 9 indicates a String
                value = "'" + str(value) + "'"
            if constraint is not None:
                where = "(" + str(feature) + str(op) + str(value) + ") AND " + constraint
            else:
                where = "(" + str(feature) + str(op) + str(value) + ")"
            FlowStatus(history=self.history.id, title='Fetching Data',
                       description=('Requesting bigquery to obtain the data based on the mentioned criteria - ' +
                                    where),
                       result="Pending", target_id=self.element.id, element=self.element.name).save(self.master)
            bq_output = bigquery.select_star(qualified_table_name=identifier, where=where, sync=True)
            csv = []
            fields = [field["name"] for field in bq_output['fields']]
            for row in bq_output['rows']:
                row_dict = {}
                for index, cell in enumerate(row):
                    row_dict[fields[index]] = cell["v"]
                csv.append(row_dict)
            return csv
        elif source == 'drive':
            metadata = drive.get_metadata(identifier)
            FlowStatus(history=self.history.id, title='Fetching Data',
                       description='Reading the file - ' + str(metadata['name']) + ' having identifier ' + str(
                           metadata['id']), result="Pending", target_id=self.element.id,
                       element=self.element.name).save(self.master)
            return drive.get_file_contents(identifier)
        elif source == 'galileo':
            response = self.query.results
            filesystem = response['filesystem']
            header = response['header']
            csv = []
            contents = []
            for result in response['result']:
                if result['hostName'] == hostname:
                    for filePath in result['filePath']:
                        with open(filePath, 'r') as result_file:
                            contents.extend(result_file.read().splitlines())
                        # if "/%s/" % filesystem not in filePath:
                        #     os.remove(filePath)
                else:
                    address = None
                    for worker in response['workers']:
                        if worker[0] == result['hostName']:
                            address = worker
                            break
                    if address is None:
                        raise ValueError("Failed to find the address of the host %s to fetch the result files %s" % (
                            result['hostName'], result['filePath']))
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        logger.info("Fetching data from remote host with address %s " % str(address))
                        sock.connect((address[0], address[1]))
                        request = messaging.Request(messaging.RequestType.FILE,
                                                    messaging.WorkerFileRequest(filesystem, result['filePath']))
                        contents = messaging.sendrecv(sock, request)
                    except Exception as e:
                        err_msg = "Failed to obtain the contents of the files %s from %s" % (
                            result['filePath'], result['hostName'])
                        logger.error(err_msg)
                        logger.error(traceback.format_exc())
                        raise type(e)(err_msg)
                if contents:
                    header = [str(column).split(":") for column in header]
                    columns = [str(fields[0]) for fields in header]
                    column_types = [int if str(fields[1]) in ['INT', 'INTEGER'] else
                                    float if str(fields[1]) in ['FLOAT', 'DOUBLE'] else
                                    long if str(fields[1]) == 'LONG' else str for fields in header]
                    while contents:
                        row = str(contents.pop(0)).split(",")
                        row_dict = {}
                        for index, column in enumerate(row):
                            cast = column_types[index]
                            row_dict[columns[index]] = cast(column) if not caught(cast, column) else str(column)
                        csv.append(row_dict)
            return csv
        return []

    def get_current_time(self, user_time=None):
        if not user_time:
            return datetime.now(timezone(self.history.timezone)).strftime("%a, %d %b %Y - %I:%M:%S %p")
        if isinstance(user_time, datetime):
            return user_time.replace(tzinfo=timezone(self.history.timezone)).strftime("%a, %d %b %Y - %I:%M:%S %p")
        return str(user_time)

    @staticmethod
    def get_engine_setting(setting_name):
        if isinstance(threading.current_thread(), EngineThread):
            return threading.current_thread().settings.get(setting_name, None)
        raise TypeError('engine setting must be retrieved through an engine thread')

    def run(self):
        try:
            # self.history.status = "Running"
            # self.history.save(self.master)
            if isinstance(self.element, Component) and self.element.is_root:
                __input__ = self.fetch_raw_data()
                directory = "%s%s/pickles/targets/h%d" % (EngineThread.get_engine_setting(
                    STORAGE.PERSISTENT.LOCAL), self.username, self.history.id)
                filename = "h" + str(self.history.id) + "e" + str(self.element.id) + ".pickle"
                engine_res = EngineResource(source=self.query.source, details=self.query.desc, data=__input__)
                pickle_response = engine_res.serialize(directory=directory, filename=filename)
                pickle_size = pickle_response['size']
                pickle_name = "%s/%s" % (directory, filename)
                gcs_pickle_name = "%s#%s" % (pickle_response['bucket'], pickle_response['name'])
                FlowStatus(history=self.history.id, title='Fetch Completed',
                           description='Obtained ' + str(len(__input__)) + ' rows of data',
                           result='Success', ref="engine-" + str(self.element.id), target_id=self.element.id,
                           element="Pre-Processing", local_pickle=pickle_name, global_pickle=gcs_pickle_name,
                           type='csv', pickle_size=pickle_size).save(self.master)
                self.element.sys_input = __input__
                self.execute_element()
            elif isinstance(self.element, Combiner):
                current_time = self.get_current_time()
                combiner_parent = self.input.parents[0]
                identifier = self.element.flow_name
                FlowStatus(title='Combining Data',
                           description='Combining the output of ' + identifier + ' [start:' + str(
                               self.element.start) + ', end: ' + str(self.element.end) + '] as of ' + current_time,
                           result='Pending', target_id=self.element.id,
                           element=self.element.name, history=self.history.id).save(self.master)
                local_pickles = self.input.local_pickles[combiner_parent]
                global_pickles = self.input.global_pickles[combiner_parent]
                if not local_pickles and not global_pickles:
                    desc = 'Failed to combine the data because there are no instances for the workflow - ' + \
                           identifier
                    FlowStatus(title='Combining Failed', description=desc, result='Failed',
                               target_id=self.element.id, element=self.element.name,
                               history=self.history.id).save(self.master)
                    raise ValueError("No instances found for '" + identifier + "'")
                __input__ = {'workflow': [], 'ftkey': []}
                for index, local_pickle in enumerate(local_pickles):
                    component = EngineResource.load(local_pickle, global_pickles[index])
                    __input__['workflow'].append(component.sys_output)
                    if component.ftkey:
                        __input__['ftkey'].append(component.ftkey)
                FlowStatus(title='Combining Completed',
                           description=('Gathered output from ' + str(
                               len(local_pickles)) + ' instance(s) of ' + str(identifier)), target_id=self.element.id,
                           result='Success', element=self.element.name, history=self.history.id).save(self.master)
                self.element.sys_input = __input__
                self.execute_element()
            else:
                # element is a non-root component
                FlowStatus(title='Gathering Input',
                           description='Collecting the output of all parents and combiners of this component',
                           result='Pending', target_id=self.element.id,
                           element=self.element.name, history=self.history.id).save(self.master)
                __input__ = {'ftkey': {}}
                for parent_component in self.element.sys_parents:
                    parent_key = str('component-') + str(parent_component)
                    if parent_key not in self.input.parents:
                        FlowStatus(title='Gathering Failed',
                                   description='Failed to gather input because the output of component-' + str(
                                       parent_component) + ' is missing.', result='Failed', target_id=self.element.id,
                                   element=self.element.name, history=self.history.id).save(self.master)
                        raise ValueError("Output not found for one of the parents: component-" + str(parent_component))
                    component = EngineResource.load(self.input.local_pickles[parent_key][0],
                                                    self.input.global_pickles[parent_key][0])
                    __input__[parent_key] = component.sys_output
                    if component.ftkey is not None:
                        __input__["ftkey"]['component-' + str(parent_component)] = component.ftkey

                for parent_combiner in self.element.sys_combiners:
                    combiner_key = str('combiner-') + str(parent_combiner)
                    if combiner_key not in self.input.parents:
                        FlowStatus(title='Gathering Failed',
                                   description='Failed to gather input because the output of combiner-' + str(
                                       parent_combiner) + ' is missing.', result='Failed', target_id=self.element.id,
                                   element=self.element.name, history=self.history.id).save(self.master)
                        raise ValueError("Output not found for one of the parents: combiner-" + str(parent_combiner))
                    combiner = EngineResource.load(self.input.local_pickles[combiner_key][0],
                                                   self.input.global_pickles[combiner_key][0])
                    __input__[combiner_key] = combiner.sys_output
                    if combiner.ftkey is not None:
                        __input__["ftkey"]['combiner-' + str(parent_combiner)] = combiner.ftkey
                logger.info("component's input - " + str(__input__.keys()))
                FlowStatus(title='Gathering Completed',
                           description='The output of all parents and combiners of this component has been collected',
                           result='Success', target_id=self.element.id, element=self.element.name,
                           history=self.history.id).save(self.master)
                self.element.sys_input = __input__
                self.execute_element()
            self.history.status = "Finished"
            self.history.save(self.master)
            return
        except BaseException as e:
            if isinstance(e, MemoryError):
                self.__error = e.__class__.__name__
                self.__trace = traceback.format_exc()
                return
            self.history.status = "Failed"
            self.history.save(self.master)
            exc_message = e.message
            if len(exc_message) > 9900:
                # max supported length is 10000 characters.
                exc_message = exc_message[0:9900] + str('... ***trace truncated***')
            FlowStatus(history=self.history.id, title='Task Failed',
                       description=self.history.flow_name + ' failed to execute. Reason: ' + exc_message,
                       result='Failed', target_id=self.element.id, element=self.element.name).save(self.master)
            self.__error = e.__class__.__name__
            self.__trace = exc_message
            return
        finally:
            self.active = False


class EngineProcess(multiprocessing.Process):
    def __init__(self, flow_id, num_containers, container_size, target, settings, status, master, worker):
        super(EngineProcess, self).__init__()
        self.flow_id = flow_id
        self.num_containers = num_containers
        self.container_size = container_size
        self.reserve = 0
        self.target = target
        self.target_name = target.name
        self.settings = settings
        self.status = status
        self.master = master
        self.worker = worker
        # target.history.status = "Queued"
        # target.history.save(master)

    def send_status(self):
        self.target.history.status = "Running"
        self.target.history.save(self.master)

    def run(self):
        pid = os.getpid()
        # settings.configure_logging(logger_name='worker', logfile_path="/tmp/worker-%d.log" % pid)
        try:
            if resource_loader is not None:
                import resource
                limit = self.num_containers * self.container_size * 1024 * 1024
                resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
            et = EngineThread(settings=self.settings, master=self.master, history=self.target.history,
                              element=self.target.element, query=self.target.query, input=self.target.input)
            et.start()
            while et.is_running():
                time.sleep(1)
            if et.is_successful():
                self.status["pid"] = pid
                self.status["output"] = jsonpickle.encode(et.output, unpicklable=False)
            else:
                self.status["pid"] = pid
                error, trace = et.get_error_trace()
                self.status["error"] = error
                self.status["details"] = trace
            return
        except BaseException as e:
            self.status["pid"] = pid
            self.status["error"] = e
            self.status["details"] = traceback.format_exc()
            return
        finally:
            messaging.push(self.worker, messaging.Request(messaging.RequestType.FINISHED,
                                                          messaging.TargetFinishedRequest(pid=pid)))

    def __str__(self):
        return "EngineProcess(pid=%d,flow=%d,target=%s,containers=%d" % (
            self.pid, self.flow_id, self.target_name, self.num_containers)
