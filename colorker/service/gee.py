import errno
import logging
import os
import threading
import time
import uuid

from googleapiclient import http

from colorker.security import CredentialManager, ServiceAccount
from colorker.settings import STORAGE
from colorker.utils import current_time_millis

logger = logging.getLogger('worker')


def download_object(bucket, filename, out_file, user_settings=None, access=ServiceAccount.STORAGE):
    if access == ServiceAccount.STORAGE:
        service = CredentialManager.get_client_storage_service(user_settings)
    elif access == ServiceAccount.EARTH_ENGINE:
        service = CredentialManager.get_ee_storage_service(user_settings)
    else:
        service = CredentialManager.get_server_storage_service(user_settings)
    # Use get_media instead of get to get the actual contents of the object.
    # http://g.co/dev/resources/api-libraries/documentation/storage/v1/python/latest/storage_v1.objects.html#get_media
    req = service.objects().get_media(bucket=bucket, object=filename)
    if not os.path.exists(os.path.dirname(out_file)):
        try:
            os.makedirs(os.path.dirname(out_file))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    with open(out_file, "wb") as fh:
        downloader = http.MediaIoBaseDownload(fh, req, chunksize=1024 * 1024)
        done = False
        while done is False:
            status, done = downloader.next_chunk()


def delete_object(bucket, filename, user_settings=None, access=ServiceAccount.GCS):
    if access == ServiceAccount.EARTH_ENGINE:
        service = CredentialManager.get_ee_storage_service(user_settings)
    else:
        service = CredentialManager.get_server_storage_service(user_settings)
    req = service.objects().delete(bucket=bucket, object=filename)
    resp = req.execute(num_retries=3)
    return resp


def get_bucket_metadata(bucket, user_settings=None, access=ServiceAccount.STORAGE):
    """Retrieves metadata about the given bucket."""
    if access == ServiceAccount.STORAGE:
        service = CredentialManager.get_client_storage_service(user_settings)
    elif access == ServiceAccount.EARTH_ENGINE:
        service = CredentialManager.get_earth_engine(user_settings)
    else:
        service = CredentialManager.get_server_storage_service(user_settings)
    # Make a request to buckets.get to retrieve a list of objects in the
    # specified bucket.
    req = service.buckets().get(bucket=bucket)
    return req.execute(num_retries=3)


def list_bucket(bucket, user_settings=None, access=ServiceAccount.STORAGE):
    """Returns a list of metadata of the objects within the given bucket."""
    if access == ServiceAccount.STORAGE:
        service = CredentialManager.get_client_storage_service(user_settings)
    elif access == ServiceAccount.EARTH_ENGINE:
        service = CredentialManager.get_ee_storage_service(user_settings)
    else:
        service = CredentialManager.get_server_storage_service(user_settings)
    # Create a request to objects.list to retrieve a list of objects.
    fields_to_return = 'nextPageToken,items(name,size,contentType,metadata(my-key))'
    req = service.objects().list(bucket=bucket, fields=fields_to_return)

    all_objects = []
    # If you have too many items to list in one request, list_next() will
    # automatically handle paging with the pageToken.
    while req:
        resp = req.execute(num_retries=3)
        all_objects.extend(resp.get('items', []))
        req = service.objects().list_next(req, resp)
    return all_objects


def upload_object(bucket, filename, readers, owners, user_settings=None, access=ServiceAccount.GCS):
    """
    Uploads the specified file to the specified bucket. The object path in the bucket is same as the
    path of the file specified.
    :param bucket: Name of the cloud storage bucket
    :param filename: fully qualified name of the file to upload
    :param readers: list of email addresses
    :param owners: list of email addresses
    :param user_settings: optional, a dictionary of user credentials for appropriate services.
    If one is not provided, then this method must be invoked by an EngineThread which defines the settings
    :return:
    """
    if access == ServiceAccount.EARTH_ENGINE:
        service = CredentialManager.get_ee_storage_service(user_settings)
    else:
        service = CredentialManager.get_server_storage_service(user_settings)
    # This is the request body as specified:
    # http://g.co/cloud/storage/docs/json_api/v1/objects/insert#request
    body = {
        'name': filename[1:] if filename[0] == '/' else filename,
    }
    # If specified, create the access control objects and add them to the
    # request body
    if readers or owners:
        body['acl'] = []

    for r in readers:
        body['acl'].append({
            'entity': 'user-%s' % r,
            'role': 'READER',
            'email': r
        })
    for o in owners:
        body['acl'].append({
            'entity': 'user-%s' % o,
            'role': 'OWNER',
            'email': o
        })
    # Now insert them into the specified bucket as a media insertion.
    # http://g.co/dev/resources/api-libraries/documentation/storage/v1/python/latest/storage_v1.objects.html#insert
    with open(filename, 'rb') as f:
        req = service.objects().insert(
            bucket=bucket, body=body,
            # You can also just set media_body=filename, but # for the sake of
            # demonstration, pass in the more generic file handle, which could
            # very well be a StringIO or similar.
            media_body=http.MediaIoBaseUpload(f, 'application/octet-stream'))
        resp = req.execute(num_retries=3)
    return resp


def get_geojson(ftc):
    """
    Returns the geojson representation of the ee.FeatureCollection. This function can be called only by an EngineThread
    :param ftc: an instance of ee.FeatureCollection
    """
    try:
        return ftc.getInfo()
    except:
        unique_id = str(uuid.uuid4())
        filename = str(threading.current_thread().username) + "/" + unique_id
        ee = CredentialManager.get_earth_engine()
        export_task = ee.batch.Export.table.toCloudStorage(
            ftc, description=unique_id, bucket=threading.current_thread().settings.get(STORAGE.TEMPORARY.CLOUD, None),
            fileNamePrefix=str(filename) + "/", fileFormat='GeoJSON')
        start_time = current_time_millis()
        end_time = start_time
        export_task.start()
        Task = ee.batch.Task
        max_wait_time = 7200
        wait_time = 0
        while export_task.status()['state'] in [Task.State.UNSUBMITTED, Task.State.CANCEL_REQUESTED, Task.State.READY,
                                                Task.State.RUNNING]:
            time.sleep(5)  # wait for 5 seconds before polling the status of the task.
            wait_time += 5
            if export_task.status()['state'] == Task.State.UNSUBMITTED:
                end_time = current_time_millis()
            if wait_time >= max_wait_time and export_task.status()['state'] == Task.State.UNSUBMITTED:
                export_task.cancel()
                logger.info('Task (' + str(export_task.id) + ') remained unsubmitted for ' + str(
                    (end_time - start_time) / 1000) + ' seconds. Cancelled the task')
                raise Exception('Task not submitted for more than 2 hours. Cancelled the task.')
        logger.info('Task (' + str(export_task.id) + ') submitted after ' + str(
            (end_time - start_time) / 1000) + ' seconds.')
        logger.info('Task (' + str(export_task.id) + ') ran for ' + str(
            (current_time_millis() - end_time) / 1000) + ' seconds. State: ' + str(export_task.status()['state']))
        if export_task.status()['state'] == Task.State.COMPLETED:
            from geojson import load
            out_file = threading.current_thread().settings.get(
                STORAGE.TEMPORARY.LOCAL, None) + '/' + str(filename) + '.geojson'
            download_object(threading.current_thread().settings.get(STORAGE.TEMPORARY.CLOUD, None),
                            str(filename) + '/ee_export.geojson', out_file, access=ServiceAccount.EARTH_ENGINE)
            with open(out_file, 'rb') as fh:
                return load(fh)
        else:
            if export_task.status()['state'] == Task.State.FAILED:
                raise Exception(
                    'Task failed. Could not get the feature collection from Earth Engine. Reason - ' +
                    export_task.status()[
                        'error_message'])
            raise Exception('Task cancelled. Could not get the feature collection from Earth Engine.')
