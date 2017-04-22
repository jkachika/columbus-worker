#!/usr/bin/python
#
# Author: Johnson Kachikaran (johnsoncharles26@gmail.com)
# Date: 7th August 2016
# Google Drive API:
# https://developers.google.com/drive/v3/reference/
# https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/
"""
Includes functions to integrate with a user's Google drive. The results and implementation is based on the API
provided by the Google Drive API:

https://developers.google.com/drive/v3/reference/

https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/
"""
import io
import os
import threading

from googleapiclient.http import MediaIoBaseDownload

from colorker.security import CredentialManager
from colorker.settings import STORAGE


def list_files(query=None, order_by=None, files=False, user_settings=None):
    drive_service = CredentialManager.get_client_drive_service(user_settings)
    response = drive_service.files().list(
        orderBy=order_by, q=query, pageSize=1000,
        fields='nextPageToken, files(id, name, mimeType, fileExtension, parents)').execute(num_retries=3)
    result, resources, names, parents = [], [], {}, {}
    for drive_file in response.get('files', []):
        names[str(drive_file['id'])] = str(drive_file['name'])
        parents[str(drive_file['id'])] = drive_file.get('parents', [])
        resources.append({'id': drive_file['id'], 'name': drive_file['name'],
                          'parents': [str(parent) for parent in drive_file.get('parents', [])],
                          'mimeType': drive_file['mimeType']})
    while response.get('nextPageToken', None):
        drive_files = drive_service.files()
        response = drive_files.list(orderBy=order_by, q=query, pageSize=1000, pageToken=response['nextPageToken'],
                                    fields='nextPageToken, files(id, name, mimeType, fileExtension, parents)').execute(num_retries=3)
        for drive_file in response.get('files', []):
            names[str(drive_file['id'])] = str(drive_file['name'])
            parents[str(drive_file['id'])] = drive_file.get('parents', [])
            resources.append({'id': drive_file['id'], 'name': drive_file['name'],
                              'parents': [str(parent) for parent in drive_file.get('parents', [])],
                              'mimeType': drive_file['mimeType']})
    for resource in resources:
        if resource['parents']:
            for parent in resource['parents']:
                path = str(names.get(parent, '')) + str('/') + str(resource['name'])
                while parents.get(parent, []):
                    parent = str(parents[parent][0])
                    path = str(names.get(parent, '')) + str('/') + path
                resource['name'] = path
                if files:
                    if resource['mimeType'] != 'application/vnd.google-apps.folder':
                        result.append(resource)
                else:
                    result.append(resource)
        else:
            if files:
                if resource['mimeType'] != 'application/vnd.google-apps.folder':
                    result.append(resource)
            else:
                result.append(resource)
    return result


def get_metadata(file_id, user_settings=None):
    """
    Obtains the metadata of a file

    :param str file_id: the identifier of the file whose metadata is needed
    :param dict user_settings: optional, A dictionary of settings specifying credentials for appropriate services.
                            If one is not provided, then this method must be invoked by an EngineThread
                            which defines the settings
    :return: metadata of the file including id, mimeType, size, parents, kind, fileExtension, and webContentLink
    """
    drive_service = CredentialManager.get_client_drive_service(user_settings)
    files_service = drive_service.files().get(
        fileId=file_id, fields='id, mimeType, size, parents, kind, name, fileExtension, webContentLink')
    return files_service.execute(num_retries=3)


def get_file_contents(file_id, meta_err=False, user_settings=None):
    """
    Obtains the contents of a file as a list of dictionaries. File type of the requested file must be a csv or a
    Google fusion table.

    :param str file_id: the identifier of the file whose content is needed
    :param bool meta_err: optional, internal use only
    :param dict user_settings: optional, A dictionary of settings specifying credentials for appropriate services.
                            If one is not provided, then this method must be invoked by an EngineThread
                            which defines the settings

    :return: list of dictionaries where each dictionary is a row in the file
    :rtype: list
    """
    metadata = get_metadata(file_id, user_settings)
    if (metadata.get('fileExtension', None) == 'csv' or metadata.get('mimeType', None) == 'text/csv') and metadata.get(
            'webContentLink', None):
        drive_service = CredentialManager.get_client_drive_service(user_settings)
        if user_settings is None:
            user_settings = threading.current_thread().settings
        temp_dir_path = user_settings.get(STORAGE.TEMPORARY.LOCAL, None)
        if not os.path.exists(temp_dir_path):
            os.makedirs(temp_dir_path)
        file_path = temp_dir_path + str(file_id) + ".csv"
        if not os.path.exists(file_path):
            request = drive_service.files().get_media(fileId=file_id)
            fh = io.FileIO(file_path, mode='wb')
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            fh.close()
        header, rows = [], []
        with open(file_path, 'rb') as csv_file:
            for line in csv_file.readlines():
                if not header:
                    header = [str(heading).strip() for heading in str(line).split(',')]
                else:
                    row = line.split(',')
                    row_dict = {}
                    for index, column in enumerate(row):
                        row_dict[header[index]] = str(column).strip()
                    rows.append(row_dict)
        return rows
    elif metadata.get('mimeType', None) == 'application/vnd.google-apps.fusiontable':
        ft_service = CredentialManager.get_client_fusion_table_service(user_settings)
        query = ft_service.query()
        table = query.sql(sql='SELECT * FROM ' + str(file_id), hdrs=False).execute(num_retries=3)
        result_rows = []
        columns = [str(column) for column in table['columns']]
        rows = table['rows']
        for row in rows:
            result_row = {}
            for index, cell in enumerate(row):
                result_row[columns[index]] = str(cell) if isinstance(cell, unicode) else cell
            result_rows.append(result_row)
        return result_rows
    elif meta_err:
        raise Exception('Unsupported file type for the file - ' + str(metadata['name'] + '.'))
    return []
