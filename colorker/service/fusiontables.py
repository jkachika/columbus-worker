#!/usr/bin/python
#
# Author: Johnson Kachikaran (johnsoncharles26@gmail.com)
# Date: 19th May 2016
# Fusion Tables API:
# https://developers.google.com/resources/api-libraries/documentation/fusiontables/v2/python/latest/index.html
"""
Includes functions to integrate with Google Fusion Tables. The results and implementation is based on the API
provided by the Google Fusion Tables API:

https://developers.google.com/resources/api-libraries/documentation/fusiontables/v2/python/latest/index.html
"""

import csv
import os
import threading
import traceback
import logging
import numpy as np

from area import area
from lxml import etree
from googleapiclient.http import MediaIoBaseUpload
from geojson import FeatureCollection
from pykml.factory import KML_ElementMaker as KML

from colorker.security import CredentialManager
from colorker.settings import STORAGE


logger = logging.getLogger('worker')


def create_table(name, description, columns, data=None, share_with=None, admin=None, user_settings=None):
    """
    Creates a fusion table for the given data and returns the table id.

    :param str name: Name of the fusion table to create
    :param str description: Description of the table to be created
    :param columns: List of dictionaries having properties name and type
    :type columns: list(dict)
    :param data: List of dictionaries (optional)
    :type data: list(dict)
    :param share_with: Single email addreess string or  a List of user email addresses (gmail only)
                      to share the created fusion table
    :type share_with: str or list(str)
    :param str admin: email address of the administrator who should have edit access to the created fusion table
    :param dict user_settings: optional, A dictionary of settings specifying credentials for appropriate services.
                            If one is not provided, then this method must be invoked by an EngineThread
                            which defines the settings

    :rtype: str
    :return: the table id of the created fusion table
    """
    ft_service = CredentialManager.get_fusion_tables_service(user_settings)
    drive_service = CredentialManager.get_drive_service(user_settings)

    # converting column type to fusion table supported type
    for column in columns:
        column["type"] = str(column["type"]).upper()
        column["type"] = "NUMBER" if column["type"] in ["INTEGER", "FLOAT", "NUMBER"] \
            else "DATETIME" if column["type"] in ["TIMESTAMP", "DATETIME", "DATE"] \
            else "LOCATION" if column["type"] == "LOCATION" \
            else "STRING"

    body = dict(name=name, description=description, attribution="Created by Columbus Workflow Engine",
                attributionLink="http://www.columbus.cs.colostate.edu", columns=columns, isExportable=True)
    table = ft_service.table()
    result = table.insert(body=body).execute(num_retries=3)
    table_id = result["tableId"]
    logger.info("table created with id - " + table_id)
    permissions = drive_service.permissions()
    # give write access to the admin for all the created fusion tables
    if admin is not None:
        permissions.create(fileId=table_id, body={"emailAddress": admin, "type": "user", "role": "writer"},
                           sendNotificationEmail=False).execute(num_retries=3)
    permissions.create(fileId=table_id,
                       body={"type": "anyone", "role": "reader", "allowFileDiscovery": False}).execute(num_retries=3)
    if share_with is not None:
        if isinstance(share_with, list):
            for user_email in share_with:
                if user_email.endswith("gmail.com"):
                    logger.info("setting drive permissions for user - " + user_email)
                    permissions.create(fileId=table_id,
                                       body={"emailAddress": user_email, "type": "user", "role": "reader"},
                                       sendNotificationEmail=False).execute(num_retries=3)
        if isinstance(share_with, str) and share_with.endswith("gmail.com"):
            logger.info("setting drive permissions for user - " + share_with)
            permissions.create(fileId=table_id,
                               body={"emailAddress": share_with, "type": "user", "role": "reader"},
                               sendNotificationEmail=False).execute(num_retries=3)
    if data is not None:
        keys = [column["name"] for column in columns]
        if user_settings is None:
            user_settings = threading.current_thread().settings
        temp_dir_path = user_settings.get(STORAGE.TEMPORARY.LOCAL, None)
        if not os.path.exists(temp_dir_path):
            os.makedirs(temp_dir_path)
        filename = temp_dir_path + str(table_id) + ".csv"
        with open(filename, 'wb') as upload_file:
            dict_writer = csv.DictWriter(upload_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)
        logger.info("created temporary file for upload. making call to import rows.")
        upload_fd = open(filename, 'rb')
        media_body = MediaIoBaseUpload(fd=upload_fd, mimetype="application/octet-stream")
        result = table.importRows(tableId=table_id, media_body=media_body, startLine=1, isStrict=True,
                                  encoding="UTF-8", delimiter=",").execute(num_retries=3)
        logger.info("imported - " + str(result["numRowsReceived"]) + " rows")
    return table_id


def create_ft_from_ftc(name, description, ftc, parties=None, admin=None, user_settings=None):
    if isinstance(ftc, FeatureCollection) and ftc.get("columns", None) and isinstance(ftc["columns"], dict):
        fields = sorted(ftc["columns"].keys())
        columns = [{"name": str(field), "type": str(ftc["columns"][field])} for field in fields]
        columns.append(
            {"name": "x__geometry__x", "type": "LOCATION"})  # special property to access fusion table from maps API
        data = []
        for feature in ftc["features"]:
            if feature["type"] == "Feature":
                ft_prop = feature["properties"]
                if feature["geometry"]["type"] == "Point":
                    point = feature["geometry"]["coordinates"]
                    location = KML.Point(KML.coordinates(str(point[0]) + "," + str(point[1])))
                    ft_prop["x__geometry__x"] = etree.tostring(location)
                elif feature["geometry"]["type"] == "MultiPoint":
                    multipoint = feature["geometry"]["coordinates"]
                    geometries = [KML.Point(KML.coordinates(str(point[0]) + "," + str(point[1]))) for point in
                                  multipoint]
                    location = KML.MultiGeometry()
                    for geometry in geometries:
                        location.append(geometry)
                    ft_prop["x__geometry__x"] = etree.tostring(location)
                elif feature["geometry"]["type"] == "Polygon":
                    polygon = feature["geometry"]["coordinates"]
                    location = KML.Polygon()
                    for index in range(len(polygon)):
                        if index == 0:
                            location.append(KML.outerBoundaryIs(KML.LinearRing(KML.coordinates(
                                " ".join([str(point[0]) + "," + str(point[1]) for point in polygon[index]])))))
                        else:
                            location.append(KML.innerBoundaryIs(KML.LinearRing(KML.coordinates(
                                " ".join([str(point[0]) + "," + str(point[1]) for point in polygon[index]])))))
                    ft_prop["x__geometry__x"] = etree.tostring(location)
                elif feature["geometry"]["type"] == "MultiPolygon":
                    multipolygon = feature["geometry"]["coordinates"]
                    location = KML.MultiGeometry()
                    for polygon in multipolygon:
                        kml = KML.Polygon()
                        for index in range(len(polygon)):
                            if index == 0:
                                kml.append(KML.outerBoundaryIs(KML.LinearRing(KML.coordinates(
                                    " ".join([str(point[0]) + "," + str(point[1]) for point in polygon[index]])))))
                            else:
                                kml.append(KML.innerBoundaryIs(KML.LinearRing(KML.coordinates(
                                    " ".join([str(point[0]) + "," + str(point[1]) for point in polygon[index]])))))
                        location.append(kml)
                    ft_prop["x__geometry__x"] = etree.tostring(location)
                elif feature["geometry"]["type"] == "LineString":
                    linestring = feature["geometry"]["coordinates"]
                    location = KML.LineString(
                        KML.coordinates(" ".join([str(point[0]) + "," + str(point[1]) for point in linestring])))
                    ft_prop["x__geometry__x"] = etree.tostring(location)
                elif feature["geometry"]["type"] == "MultiLineString":
                    multilinestring = feature["geometry"]["coordinates"]
                    location = KML.MultiGeometry()
                    for linestring in multilinestring:
                        location.append(KML.LineString(
                            KML.coordinates(" ".join([str(point[0]) + "," + str(point[1]) for point in linestring]))))
                    ft_prop["x__geometry__x"] = etree.tostring(location)
                str_prop = {}
                for key in ft_prop.keys():
                    str_prop[str(key) if isinstance(key, unicode) else key] = str(ft_prop[key]) if isinstance(
                        ft_prop[key], unicode) else ft_prop[key]
                data.append(str_prop)
        return create_table(name=name, description=description, columns=columns, data=data, share_with=parties,
                            admin=admin, user_settings=user_settings)
    return None


def delete_table(table_id, user_settings=None):
    """
    Deletes a fusion table

    :param str table_id: identifier of the fusion table
    :param dict user_settings: optional, A dictionary of settings specifying credentials for appropriate services.
                            If one is not provided, then this method must be invoked by an EngineThread
                            which defines the settings

    :raises BaseException: Any exception resulting from this operation
    """
    try:
        ft_keys = str(table_id).split(',')
        for key in ft_keys:
            ft_service = CredentialManager.get_fusion_tables_service(user_settings)
            table = ft_service.table()
            table.delete(tableId=key).execute(num_retries=3)
    except BaseException as e:
        logger.error(traceback.format_exc())
        raise e


def read_table(table_id, user_settings=None):
    """
    Reads a fusion table and returns its contants as a list of dictionaries

    :param str table_id: identifier of the fusion table
    :param dict user_settings: optional, A dictionary of settings specifying credentials for appropriate services.
                            If one is not provided, then this method must be invoked by an EngineThread
                            which defines the settings

    :raises BaseException: Any exception resulting from this operation
    """
    try:
        ft_service = CredentialManager.get_fusion_tables_service(user_settings)
        query = ft_service.query()
        table = query.sql(sql='SELECT * FROM ' + str(table_id), hdrs=False).execute(num_retries=3)
        result_rows = []
        columns = [str(column) for column in table['columns']]
        rows = table['rows']
        for row in rows:
            result_row = {}
            for index, cell in enumerate(row):
                result_row[columns[index]] = str(cell) if isinstance(cell, unicode) else cell
            result_rows.append(result_row)
        return result_rows
    except BaseException as e:
        logger.error(traceback.format_exc())
        raise e


def get_polygons_from_ft(table_id, name_attr, geometry_attr, user_settings=None):
    # finds only the first polygon with outer boundary
    rows = read_table(table_id=table_id, user_settings=user_settings)
    polygons = []
    for row in rows:
        polygon = dict(name=row[name_attr], geometry=[])
        max_polygon = []
        feature = row[geometry_attr]
        if 'type' not in feature:
            feature = feature['geometry']
        if feature["type"] == "Polygon":
            outer_boundary = feature["coordinates"][0]
            for vertex in outer_boundary:
                polygon['geometry'].append(dict(lon=vertex[0], lat=vertex[1]))
        elif feature["type"] == "MultiPolygon":
            for boundary in feature["coordinates"]:
                max_polygon.append(area({"type": "Polygon", "coordinates": boundary}))
            index = np.argmax(np.array(max_polygon))
            for vertex in feature["coordinates"][index][0]:
                polygon['geometry'].append(dict(lon=vertex[0], lat=vertex[1]))
        elif feature["type"] == "GeometryCollection":
            geometries = feature['geometries']
            for geometry in geometries:
                if geometry["type"] in ["Polygon", "MultiPolygon"]:
                    max_polygon.append(area(geometry))
                else:
                    max_polygon.append(0)
            index = np.argmax(np.array(max_polygon))
            max_polygon = []
            feature = geometries[index]
            if feature["type"] == "Polygon":
                outer_boundary = feature["coordinates"][0]
                for vertex in outer_boundary:
                    polygon['geometry'].append(dict(lon=vertex[0], lat=vertex[1]))
            elif feature["type"] == "MultiPolygon":
                for boundary in feature["coordinates"]:
                    max_polygon.append(area({"type": "Polygon", "coordinates": boundary}))
                index = np.argmax(np.array(max_polygon))
                for vertex in feature["coordinates"][index][0]:
                    polygon['geometry'].append(dict(lon=vertex[0], lat=vertex[1]))

        if len(polygon['geometry']) > 0:
            polygons.append(polygon)
    return polygons
