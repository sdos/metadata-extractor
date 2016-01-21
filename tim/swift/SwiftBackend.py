#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on Mar 17, 2015

@author: tim
'''
import logging
import swiftclient
import io
from tim.swift.SwiftConfig import swift_url, swift_user, swift_pw

class SwiftBackend(object):
	'''
	classdocs
	'''
	
	def __init__(self):
		'''
		Constructor
		'''
		self.log = logging.getLogger(__name__)
		self.log.debug('initializing...')
		self.authurl = swift_url
		self.user = swift_user
		self.key = swift_pw
###############################################################################
###############################################################################

	def _getConnection(self):
		return swiftclient.client.Connection(authurl=self.authurl, user=self.user, key=self.key, retries=1, insecure='true')
	
		
###############################################################################
###############################################################################			
		
	# Retrieves list of all objects of the specified container
	#@exception_wrapper(404, "requested resource does not exist", log)
	def get_object_list(self, container_name, limit=None, marker=None, prefix=None):
		self.log.debug("Retrieving list of all objects of container: {} with parameter: limit = {}, marker = {}, prefix = {}"
				.format(container_name, limit, marker, prefix))
		conn = self._getConnection()
		full_listing = limit is None  # bypass default limit of 10.000 of swift-client
		files = conn.get_container(
			container_name, marker=marker, limit=limit, prefix=prefix,
			full_listing=full_listing)
		return files[1]
	
	
	def getObjBlob(self, conn, containerName, objName):
		r = conn.get_object(container=containerName, obj=objName, resp_chunk_size=None, query_string=None, response_dict=None, headers=None)
		b = io.BytesIO(r[1])
		return b
	
	def writeObjMetaData(self, conn, containerName, objName, metaDict):
		self.log.debug('updating object metadata in swift. updating obj {} in container {}; adding {}'.format(objName, containerName, metaDict))
		#{'content-type':'application/octet-stream'}
		conn.post_object(container=containerName, obj=objName, headers={'x-object-meta-fuuuuuuu':'application/octet-stream'}, response_dict=None)
		return '{} / {}'.format(containerName, objName)
			
