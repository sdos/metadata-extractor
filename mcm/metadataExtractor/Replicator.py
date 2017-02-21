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

colprfx="md_"
def extractMetadataFromObject(conn,containerName,objectName,filterName,filterTags):
	header = conn.head_object(container=containerName, obj=objectName, headers=None)
	sqlVals = {}
	sqlVals["extractionDate"] = datetime.datetime.now(dateutil.tz.tzutc())
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


def replicateMetadata(conn,containerName,objectName,objectType,postgreConn):
	#always insert into SwiftInternal table
	tags=["content-type", "content-length", "last-modified"]
	sqlVals=extractMetadataFromObject(conn,containerName,objectName,"SwiftInternal",tags)
	table=deriveTableName("SwiftInternal")
	upsertIntoDB(sqlVals,table,postgreConn)

	#try to find content type specific filter
	try:
		thisFilter = ImportFilter.getFilterForObjType(objectType)
	except:
		raise NoFilterFoundException("{}-{}".format(objectName, objectType))

	#try to insert into content type specific table
	sqlVals=extractMetadataFromObject(conn,containerName,objectName,thisFilter.myName,thisFilter.myValidTagNames)

	#only insert into db if content type has been indentified and metadata has been extracted
	if len(set(sqlVals.values()))>3:
		table=deriveTableName(thisFilter.myName)
		upsertIntoDB(sqlVals,table,postgreConn)

	return "Metadata of {} in {} was replicated".format(objectName,containerName)

def deriveTableName(filterName):
	return "filter_" + filterName

def upsertIntoDB(sqlVals,tableName,postgreConn):
	fields = colprfx+(", "+colprfx).join(sqlVals.keys())
	fields = fields.replace("-","_")
	values = ', '.join(['%%(%s)s' % x for x in sqlVals])

	query = "INSERT INTO "+tableName+" "+('(%s) VALUES (%s)' % (fields, values))+" ON CONFLICT ON CONSTRAINT pk_"+tableName+" DO UPDATE SET "
	updateSet=""
	for field in sqlVals.keys():
		flabel=colprfx+field.replace("-","_")
		updateSet+=flabel+"=excluded."+flabel+","
	query=query+updateSet.rstrip(',')

	#print("sql: {}".format(query))
	with postgreConn as conn:
		with conn.cursor() as cursor:
			cursor.execute(query, sqlVals)
		postgreConn.commit()

def createTablesIfAbsent(postgreConn):
	with postgreConn as conn:
		with conn.cursor() as cursor:
			internalTags=["content-type", "content-length", "last-modified"]
			createTableIfAbsent(cursor,"SwiftInternal",internalTags)
			for filter in ImportFilter.mapping.values():
				createTableIfAbsent(cursor,filter.myName,filter.myValidTagNames)
		postgreConn.commit()

def createTableIfAbsent(cursor, filterName, tags):
	tableName=deriveTableName(filterName)
	constants ="{0}containerName TEXT, {0}name TEXT, {0}extractionDate TEXT, ".format(colprfx)
	cols= constants+colprfx+(" TEXT, "+colprfx).join(tags)+" TEXT"
	cols=cols.replace("-","_")

	if "SwiftInternal"==filterName:
		fkey=""
	else:
		fkey=", FOREIGN KEY ({0}containerName, {0}name) REFERENCES filter_SwiftInternal ({0}containerName, {0}name)".format(colprfx)
	tableQuery="CREATE TABLE IF NOT EXISTS "+tableName+" ("+cols+", CONSTRAINT pk_"+tableName+" PRIMARY KEY ({0}containerName, {0}name)".format(colprfx)+fkey+")"
	logging.info("Creating {}".format(tableName))
	#print("sql: {}".format(tableQuery))
	#cursor.execute("DROP TABLE "+tableName +" CASCADE")
	cursor.execute(tableQuery)
