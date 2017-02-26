#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Metadata Extractor - identify content type, extract metadata with specific filter plugins


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""
import logging
from mcm.metadataExtractor import ImportFilter, configuration
from mcm.metadataExtractor.Exceptions import NoFilterFoundException
import psycopg2
import psycopg2.errorcodes
import datetime, dateutil

colprfx = "md_"

internal_table_objs = {"name": "SwiftInternal", "columns": ["content-type", "content-length", "last-modified"]}
internal_table_containers = {"name": "ContainerInfo", "columns": ["container-object-count", "container-bytes-used",
                                                                  "container-meta-sdosheight",
                                                                  "container-meta-sdosencryption",
                                                                  "container-meta-sdoskeycascade",
                                                                  "container-meta-sdosmasterkey",
                                                                  "container-meta-sdospartitionbits"]}


def extract_metadata_from_container(container_info):
    sqlVals = {}
    sqlVals["extractionDate"] = datetime.datetime.now(dateutil.tz.tzutc())
    sqlVals["containerName"] = container_info["name"]
    sqlVals["name"] = ""
    sqlVals["container_object_count"] = container_info.get("x-container-object-count")
    sqlVals["container_bytes-used"] = container_info.get("x-container-bytes-used")
    sqlVals["container-meta-sdosheight"] = container_info.get("x-container-meta-sosheight")
    sqlVals["container-meta-sdosencryption"] = container_info.get("x-container-meta-sdosencryption")
    sqlVals["container-meta-sdoskeycascade"] = container_info.get("x-container-meta-sdoskeycascade")
    sqlVals["container-meta-sdosmasterkey"] = container_info.get("x-container-meta-sdosmasterkey")
    sqlVals["container-meta-sdospartitionbits"] = container_info.get("x-container-meta-sdospartitionbits")
    return sqlVals


def replicate_container_info(postgres_connection, container_info):
    table_name = deriveTableName(internal_table_containers["name"])
    for this_container in container_info:
        values = extract_metadata_from_container(this_container)
        execute_db_insert(values, table_name, postgres_connection)


def extractMetadataFromObject(conn, containerName, objectName, filterName, filterTags):
    header = conn.head_object(container=containerName, obj=objectName, headers=None)
    sqlVals = {}
    sqlVals["extractionDate"] = datetime.datetime.now(dateutil.tz.tzutc())
    sqlVals["containerName"] = containerName
    sqlVals["name"] = objectName
    for tag in filterTags:
        if filterName == internal_table_objs["name"]:
            key = tag
        else:
            key = 'x-object-meta-filter-{}-{}'.format(filterName, tag)

        if key in header:
            sqlVals[tag] = header[key]
        else:
            sqlVals[tag] = ""
    return sqlVals


def replicateMetadata(conn, containerName, objectName, objectType, postgreConn):
    # always insert into SwiftInternal table
    sqlVals = extractMetadataFromObject(conn, containerName, objectName, internal_table_objs["name"],
                                        internal_table_objs["columns"])
    table = deriveTableName("SwiftInternal")
    execute_db_insert(sqlVals, table, postgreConn)

    # try to find content type specific filter
    thisFilter = ImportFilter.getFilterForObjType(objectType)

    # try to insert into content type specific table
    sqlVals = extractMetadataFromObject(conn, containerName, objectName, thisFilter.myName, thisFilter.myValidTagNames)

    # only insert into db if content type has been indentified and metadata has been extracted
    if len(set(sqlVals.values())) > 3:
        table = deriveTableName(thisFilter.myName)
        execute_db_insert(sqlVals, table, postgreConn)

    return "Metadata of {} in {} was replicated".format(objectName, containerName)


def deriveTableName(filterName):
    return "filter_" + filterName


def execute_db_insert(sqlVals, tableName, postgreConn):
    fields = colprfx + (", " + colprfx).join(sqlVals.keys())
    fields = fields.replace("-", "_")
    values = ', '.join(['%%(%s)s' % x for x in sqlVals])

    query = "INSERT INTO " + tableName + " " + (
        '(%s) VALUES (%s)' % (fields, values)) + " ON CONFLICT ON CONSTRAINT pk_" + tableName + " DO UPDATE SET "
    updateSet = ""
    for field in sqlVals.keys():
        flabel = colprfx + field.replace("-", "_")
        updateSet += flabel + "=excluded." + flabel + ","
    query = query + updateSet.rstrip(',')

    # print("sql: {}".format(query))
    with postgreConn as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, sqlVals)
        postgreConn.commit()


def createTablesIfAbsent(postgreConn):
    with postgreConn as conn:
        with conn.cursor() as cursor:
            create_internal_table(cursor, internal_table_objs)
            create_internal_table(cursor, internal_table_containers)
            for filter in ImportFilter.mapping.values():
                create_filter_table(cursor, filter.myName, filter.myValidTagNames)
        postgreConn.commit()


def create_internal_table(cursor, table_def):
    tableName = deriveTableName(table_def["name"])
    tags = table_def["columns"]
    constants = "{0}containerName TEXT, {0}name TEXT, {0}extractionDate TEXT, ".format(colprfx)
    cols = constants + colprfx + (" TEXT, " + colprfx).join(tags) + " TEXT"
    cols = cols.replace("-", "_")

    tableQuery = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + cols + ", CONSTRAINT pk_" + tableName + " PRIMARY KEY ({0}containerName, {0}name)".format(
        colprfx) + ")"
    logging.info("Creating {}".format(tableName))
    # print("sql: {}".format(tableQuery))
    # cursor.execute("DROP TABLE "+tableName +" CASCADE")
    cursor.execute(tableQuery)


def create_filter_table(cursor, filterName, tags):
    tableName = deriveTableName(filterName)
    constants = "{0}containerName TEXT, {0}name TEXT, {0}extractionDate TEXT, ".format(colprfx)
    cols = constants + colprfx + (" TEXT, " + colprfx).join(tags) + " TEXT"
    cols = cols.replace("-", "_")

    fkey = ", FOREIGN KEY ({0}containerName, {0}name) REFERENCES filter_SwiftInternal ({0}containerName, {0}name)".format(
        colprfx)
    tableQuery = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + cols + ", CONSTRAINT pk_" + tableName + " PRIMARY KEY ({0}containerName, {0}name)".format(
        colprfx) + fkey + ")"
    logging.info("Creating {}".format(tableName))
    # print("sql: {}".format(tableQuery))
    # cursor.execute("DROP TABLE "+tableName +" CASCADE")
    cursor.execute(tableQuery)
