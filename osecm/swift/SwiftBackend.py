#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on Mar 17, 2015

@author: osecm
'''
import logging
import swiftclient
import io

class SwiftBackend(object):
	'''
	classdocs
	'''
	
	def __init__(self, swift_url, swift_user, swift_pw):
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
		

	def removeInternalMetadata(self, mdIn):
		mdOut = mdIn.copy()
		for k in mdIn.keys():
			if not k.startswith('x-object-meta-'):
				mdOut.pop(k)
		return mdOut
		
#############################################################################
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
	
	
	def updateMetaDataFields(self, conn, containerName, objName, metaDict):
		self.log.debug('updating object metadata in swift. updating obj {} in container {}; updating {}'.format(objName, containerName, metaDict))
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
