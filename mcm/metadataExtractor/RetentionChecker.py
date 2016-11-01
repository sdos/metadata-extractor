#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	RetentionChecker - check the retention date of objects, delete if they're old


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""
import datetime
import logging

import dateutil.parser

from mcm.metadataExtractor.Exceptions import RetentionDateInFutureException, NoRetentionDateException

RETENTIONFIELD = 'x-object-meta-mgmt-retentiondate'

log = logging.getLogger()


##############################################################################
# swift api
##############################################################################
def checkRetentionDate(conn, containerName, objectName):
	d = getRetentionDate(conn, containerName, objectName)
	if isRetentionOver(d):
		log.warning("DELETING - retention over on obj: {}-{}-{}".format(containerName, objectName, d))
		return conn.delete_object(containerName, objectName)
	else:
		raise(RetentionDateInFutureException("Retention NOT expired: {}-{}-{}".format(d, containerName, objectName)))


def getRetentionDate(conn, containerName, objectName):
	h = conn.head_object(containerName, objectName)
	t = h.get(RETENTIONFIELD, None)
	if t:
		try:
			d = dateutil.parser.parse(t)
			return d
		except Exception as e:
			log.exception(
				"could not parse retention date -- {} -- on obj: {} in {}".format(t, objectName, containerName))
			raise(e)

	raise(NoRetentionDateException("{}-{}".format(containerName, objectName)))


def isRetentionOver(retentionDate):
	now = datetime.datetime.now(dateutil.tz.tzutc())
	return retentionDate < now
