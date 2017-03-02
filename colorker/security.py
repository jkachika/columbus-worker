import threading

import ee
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client.client import Credentials
from oauth2client.service_account import ServiceAccountCredentials

from colorker.settings import CREDENTIALS


class MissingCredentialsException(Exception):
    """Error raised when a credentials instance cannot be created because of lack of information on current thread"""


class ServiceAccount:
    BIG_QUERY = 'bigquery'
    FUSION_TABLES = 'fusiontables'
    STORAGE = 'storage'
    GCS = 'storage_rw'  # for storing the pickle files
    DRIVE = 'drive'
    EARTH_ENGINE = 'earthengine'

    def __init__(self):
        pass


class ClientCredentials:
    DRIVE = 'user_drive'

    def __init__(self):
        pass


class CredentialManager:
    def __init__(self):
        pass

    @staticmethod
    def __build_credentials(service_name, scopes, user_settings=None):
        """
        internal use only. returns a credentials instance from the security information available on current thread
        :param service_name - string, could be one of bigquery, fusiontables, earthengine, drive, storage, storage_rw
        :param scopes - list, list of scopes to which this credentials instance should have access
        :except MissingCredentialsException
        """
        if user_settings is None:
            user_settings = threading.current_thread().settings
        server_credentials = user_settings.get(CREDENTIALS.SERVER, None)
        if not server_credentials:
            raise MissingCredentialsException
        credentials_json = server_credentials.get(service_name, None)
        if not credentials_json:
            raise MissingCredentialsException
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_json, scopes=scopes)
        return credentials

    @staticmethod
    def __build_client_credentials(service_name, user_settings=None):
        if user_settings is None:
            user_settings = threading.current_thread().settings
        client_credentials = user_settings.get(CREDENTIALS.CLIENT, None)
        if not client_credentials:
            raise MissingCredentialsException
        credentials_json = client_credentials.get(service_name, None)
        if not credentials_json:
            raise MissingCredentialsException
        return Credentials.new_from_json(json_data=credentials_json)

    @staticmethod
    def get_big_query_service(user_settings=None):
        scopes = ['https://www.googleapis.com/auth/bigquery']
        credentials = CredentialManager.__build_credentials(ServiceAccount.BIG_QUERY, scopes, user_settings)
        http_auth = credentials.authorize(Http())
        return build(serviceName=ServiceAccount.BIG_QUERY, version='v2', http=http_auth, credentials=credentials)

    @staticmethod
    def get_earth_engine(user_settings=None):
        scopes = ['https://www.googleapis.com/auth/earthengine',
                  'https://www.googleapis.com/auth/devstorage.full_control']
        credentials = CredentialManager.__build_credentials(ServiceAccount.EARTH_ENGINE, scopes, user_settings)
        ee.Initialize(credentials)
        return ee

    @staticmethod
    def get_fusion_tables_service(user_settings=None):
        scopes = ['https://www.googleapis.com/auth/fusiontables',
                  'https://www.googleapis.com/auth/drive']
        credentials = CredentialManager.__build_credentials(ServiceAccount.FUSION_TABLES, scopes, user_settings)
        http_auth = credentials.authorize(Http())
        return build(serviceName=ServiceAccount.FUSION_TABLES, version='v2', http=http_auth, credentials=credentials)

    @staticmethod
    def get_drive_service(user_settings=None):
        scopes = ['https://www.googleapis.com/auth/drive']
        credentials = CredentialManager.__build_credentials(ServiceAccount.DRIVE, scopes, user_settings)
        http_auth = credentials.authorize(Http())
        return build(serviceName=ServiceAccount.DRIVE, version='v3', http=http_auth, credentials=credentials)

    @staticmethod
    def get_client_storage_service(user_settings=None):
        scopes = ['https://www.googleapis.com/auth/devstorage.read-only']
        credentials = CredentialManager.__build_credentials(ServiceAccount.STORAGE, scopes, user_settings)
        http_auth = credentials.authorize(Http())
        return build(serviceName=ServiceAccount.STORAGE, version='v1', http=http_auth, credentials=credentials)

    @staticmethod
    def get_ee_storage_service(user_settings=None):
        scopes = ['https://www.googleapis.com/auth/earthengine',
                  'https://www.googleapis.com/auth/devstorage.full_control']
        credentials = CredentialManager.__build_credentials(ServiceAccount.EARTH_ENGINE, scopes, user_settings)
        http_auth = credentials.authorize(Http())
        return build(serviceName=ServiceAccount.STORAGE, version='v1', http=http_auth, credentials=credentials)

    @staticmethod
    def get_server_storage_service(user_settings=None):
        scopes = ['https://www.googleapis.com/auth/devstorage.full_control']
        credentials = CredentialManager.__build_credentials(ServiceAccount.GCS, scopes, user_settings)
        http_auth = credentials.authorize(Http())
        return build(serviceName=ServiceAccount.STORAGE, version='v1', http=http_auth, credentials=credentials)

    @staticmethod
    def get_client_drive_service(user_settings=None):
        credentials = CredentialManager.__build_client_credentials(ClientCredentials.DRIVE, user_settings)
        http_auth = credentials.authorize(Http())
        return build(serviceName=ServiceAccount.DRIVE, version='v3', http=http_auth, credentials=credentials)

    @staticmethod
    def get_client_fusion_table_service(user_settings=None):
        credentials = CredentialManager.__build_client_credentials(ClientCredentials.DRIVE, user_settings)
        http_auth = credentials.authorize(Http())
        return build(serviceName=ServiceAccount.FUSION_TABLES, version='v2', http=http_auth, credentials=credentials)
