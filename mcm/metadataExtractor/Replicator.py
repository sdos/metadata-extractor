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

log = logging.getLogger()

def extractMetadataFromObject(conn,containerName,objectName,filterName,filterTags):
	header = conn.head_object(container=containerName, obj=objectName, headers=None)
	sqlVals = {}
	sqlVals["containerName"]=containerName
	sqlVals["name"]=objectName
	for tag in filterTags:
		if filterName == "SwiftInternal":
			key=tag
		else:
			key='x-object-meta-filter-{}-{}'.format(filterName,tag)

		if key in header:
			sqlVals[tag]=header[key]
		else:
			sqlVals[tag]=""
	return sqlVals


def replicateMetadata(conn,containerName,objectName,objectType):
	#always insert into SwiftInternal table
	tags=["content-type", "content-length", "last-modified", "extractionDate"]
	sqlVals=extractMetadataFromObject(conn,containerName,objectName,"SwiftInternal",tags)
	table=deriveTableName(conn.user,"SwiftInternal")
	upsertIntoDB(sqlVals,table)

	#try to find content type specific filter
	try:
		thisFilter = ImportFilter.getFilterForObjType(objectType)
	except:
		raise NoFilterFoundException("{}-{}".format(objectName, objectType))

	#try to insert into content type specific table
	sqlVals=extractMetadataFromObject(conn,containerName,objectName,thisFilter.myName,thisFilter.myValidTagNames)
	#only insert into db if content type has been indentified and metadata has been extracted
	if len(set(sqlVals.values()))>3:
		table=deriveTableName(conn.user,thisFilter.myName)
		upsertIntoDB(sqlVals,table)

	return "Metadata of {} in {} was replicated".format(objectName,containerName)

def deriveTableName(swift_user,filterName):
	#TODO sql injection via user -> tableName?
	if ":" in swift_user:
		return "MetadataTable_{}_{}".format(swift_user.split(":")[0],filterName)
	else:
		return "MetadataTable_{}_{}".format(swift_user,filterName)

def upsertIntoDB(sqlVals,tableName):
	#assuming sqlVals.keys() is not an user input
	fields = "MD_"+(", MD_".join(sqlVals.keys()))
	fields = fields.replace("-","_")
	values = ', '.join(['%%(%s)s' % x for x in sqlVals])
	#TODO instead of ignoring conflicts, update all column data?
	query = "INSERT INTO "+tableName+" "+('(%s) VALUES (%s)' % (fields, values))+" ON CONFLICT ON CONSTRAINT pk_"+tableName+" DO UPDATE SET "
	updateSet=""
	for field in sqlVals.keys():
		updateSet+="MD_"+field.replace("-","_")+"=excluded."+"MD_"+field.replace("-","_")+","
	query=query+updateSet.rstrip(',')
	print("sql: {}".format(query))
	with psycopg2.connect(**configuration.metadata_warehouse_endpoint) as conn:
		conn.autocommit = True
		with conn.cursor() as cursor:
			try:
				cursor.execute(query, sqlVals)
			except psycopg2.ProgrammingError as e:
				if e.pgcode==psycopg2.errorcodes.UNDEFINED_TABLE:
					try:
						colsOrdered=sqlVals.copy()
						colsOrdered.pop("containerName")
						colsOrdered.pop("name")
						#CREATE TABLE MetaDataTable_SwiftInternal(MD_containerName, MD_name, MD_content_type, MD_content_length,
						# MD_last_modified, MD_extractionDate TEXT, CONSTRAINT pk PRIMARY KEY (MD_containerName, MD_name))
						cols= "MD_containerName TEXT, MD_name TEXT, MD_"+(" TEXT, MD_".join(colsOrdered.keys()))+" TEXT"
						cols=cols.replace("-","_")
						tableQuery="CREATE TABLE "+tableName+" ("+cols+", CONSTRAINT pk_"+tableName+" PRIMARY KEY (MD_containerName, MD_name))"
						#print("sql: {}".format(tableQuery))
						log.info("Creating {}".format(tableName))
						cursor.execute(tableQuery)
						cursor.execute(query, sqlVals)
					except psycopg2.ProgrammingError as e:
						if e.pgcode==psycopg2.errorcodes.DUPLICATE_TABLE:
							log.info("Race condition caused concurrent CREATE TABLE queries")
							cursor.execute(query, sqlVals)
						else:
							raise e
				else:
					raise e
			finally:
				cursor.close()
	return
