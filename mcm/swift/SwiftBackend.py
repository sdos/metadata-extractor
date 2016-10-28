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
import swiftclient
import io


class SwiftBackend(object):
	'''
	classdocs
	'''

	def __init__(self, swift_user=None, swift_pw=None, swift_url=None, token=None, storage_url=None):
		'''
		Constructor
		'''
		self.log = logging.getLogger(__name__)
		self.log.debug('initializing...')
		self.authurl = swift_url
		self.user = swift_user
		self.key = swift_pw
		self.token = token
		self.storage_url = storage_url

	###############################################################################
	###############################################################################

	def _getConnection(self):
		if self.token and self.storage_url:
			return swiftclient.client.Connection(
				preauthtoken=self.token,
				preauthurl=self.storage_url
			)
		return swiftclient.client.Connection(authurl=self.authurl, user=self.user, key=self.key, retries=1,
		                                     insecure='true')

	###############################################################################
	###############################################################################


	def removeInternalMetadata(self, mdIn):
		mdOut = mdIn.copy()
		for k in mdIn.keys():
			if not k.startswith('x-object-meta-'):
				mdOut.pop(k)
		return mdOut

	#############################################################################
	###############################################################################

	# Retrieves list of all objects of the specified container
	# @exception_wrapper(404, "requested resource does not exist", log)
	def get_object_list(self, container_name, limit=None, marker=None, prefix=None):
		self.log.debug(
			"Retrieving list of all objects of container: {} with parameter: limit = {}, marker = {}, prefix = {}"
				.format(container_name, limit, marker, prefix))
		conn = self._getConnection()
		full_listing = limit is None  # bypass default limit of 10.000 of swift-client
		files = conn.get_container(
			container_name, marker=marker, limit=limit, prefix=prefix,
			full_listing=full_listing)
		return files[1]

	def getObjBlob(self, conn, containerName, objName):
		r = conn.get_object(container=containerName, obj=objName, resp_chunk_size=None, query_string=None,
		                    response_dict=None, headers=None)
		b = io.BytesIO(r[1])
		return b

	def updateMetaDataFields(self, conn, containerName, objName, metaDict):
		self.log.debug('updating object metadata in swift. updating obj {} in container {}; updating {}'.format(objName,
		                                                                                                        containerName,
		                                                                                                        metaDict))
		oldHeader = conn.head_object(container=containerName, obj=objName, headers=None)
		oldHeader = self.removeInternalMetadata(oldHeader)
		newHeader = dict(oldHeader, **metaDict)
		conn.post_object(container=containerName, obj=objName, headers=newHeader, response_dict=None)
		return '{} / {} : {}'.format(containerName, objName, newHeader)

	def updateObjContentType(self, conn, containerName, objName, newContentType):
		h = dict()
		h['content-type'] = newContentType
		self.updateMetaDataFields(conn, containerName, objName, h)
		return '{} / {} : {}'.format(containerName, objName, h)
