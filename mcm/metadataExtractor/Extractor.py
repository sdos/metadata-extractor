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
from mcm.metadataExtractor.ImportFilterImages import ImportFilterBmp
from mcm.metadataExtractor.ImportFilterImages import ImportFilterGif
from mcm.metadataExtractor.ImportFilterImages import ImportFilterJpeg
from mcm.metadataExtractor.ImportFilterImages import ImportFilterPng
from mcm.metadataExtractor.ImportFilterImages import ImportFilterTiff
from mcm.metadataExtractor.ImportFilterDocuments import ImportFilterEmail
from mcm.swift.SwiftBackend import SwiftBackend
import swiftclient.multithreading
import concurrent.futures
from mcm.metadataExtractor.ContentTypeIdentifier import ContentTypeIdentifier
from mcm.metadataExtractor.Exceptions import NoFilterFoundException
from mcm.metadataExtractor.ImportFilterDocuments import ImportFilterPDF


class Extractor(object):
	'''
	classdocs
	'''
	mapping = dict()
	# image filters
	mapping[ImportFilterBmp.myContentType] = ImportFilterBmp
	mapping[ImportFilterGif.myContentType] = ImportFilterGif
	mapping[ImportFilterJpeg.myContentType] = ImportFilterJpeg
	mapping[ImportFilterPng.myContentType] = ImportFilterPng
	mapping[ImportFilterTiff.myContentType] = ImportFilterTiff

	# document filters
	mapping[ImportFilterEmail.myContentType] = ImportFilterEmail
	mapping[ImportFilterPDF.myContentType] = ImportFilterPDF

	def __init__(self, containerName, swift_url = None, swift_user = None, swift_pw = None, storage_url = None, token = None):
		'''
		Constructor
		'''
		self.log = logging.getLogger()
		self.containerName = containerName
		self.log.info('initializing...')
		if storage_url and token:
			self.sb = SwiftBackend(storage_url = storage_url, token = token)
		else:
			self.sb = SwiftBackend(swift_url=swift_url, swift_user=swift_user, swift_pw=swift_pw)
		self.numWorkers = 20

	def getFilterForObjType(self, objType):
		return self.mapping[objType]()

	def dummyLoad(self, conn, objType, objName):
		print(objName)
		thisObjBlob = self.sb.getObjBlob(conn, self.containerName, objName)

	def getDataAndIdentifyContentType(self, conn, objType, objName):
		thisObjBlob = self.sb.getObjBlob(conn, self.containerName, objName)
		ctype = ContentTypeIdentifier().identifyContentType(thisObjBlob)
		if objType == ctype:
			return "same same..."
		return self.sb.updateObjContentType(conn, containerName=self.containerName, objName=objName,
		                                    newContentType=ctype)

	def getDataAndRunFilter(self, conn, objType, objName):
		thisObjBlob = self.sb.getObjBlob(conn, self.containerName, objName)
		try:
			thisFilter = self.getFilterForObjType(objType)
		except:
			raise NoFilterFoundException("No Filter for type {}".format(objType))
		r = thisFilter.extractMetaData(thisObjBlob)
		return self.sb.updateMetaDataFields(conn=conn, containerName=self.containerName, objName=objName, metaDict=r)

	def runForWholeContainer(self, functionOnObject):
		with swiftclient.multithreading.ConnectionThreadPoolExecutor(self.sb._getConnection,
		                                                             max_workers=self.numWorkers) as executor:
			objs = self.sb.get_object_list(self.containerName)
			future_results = []

			# first go through all objs in the container and spawn a thread to run the filter
			self.log.error('committing {} jobs for {}'.format(len(objs), functionOnObject.__name__))
			for thisObj in objs:
				try:
					thisObjType = thisObj['content_type']
					thisObjName = thisObj['name']

					self.log.info('running {} for type: {} on obj: {}'.format(functionOnObject.__name__, thisObjType,
					                                                           thisObjName))
					future_results.append(executor.submit(functionOnObject, thisObjType, thisObjName))
				except Exception as exc:
					self.log.warning('could not create job for obj: {}. Exc: {}'.format(thisObj, exc))

			# try to get the individual results from the filters
			self.log.error('Starting {} worker threads...'.format(self.numWorkers))
			numFailedJobs = 0
			numNoFilter = 0
			numOkJobs = 0
			for future in concurrent.futures.as_completed(future_results):
				try:
					data = future.result()
				except NoFilterFoundException as exc:
					self.log.warning('no filter found: {}'.format(exc))
					numNoFilter += 1
				except Exception as exc:
					self.log.warning('worker failed with exception: {}'.format(exc))
					numFailedJobs += 1
				else:
					numOkJobs += 1
					self.log.debug('worker succeeded on obj: {}'.format(data))
			self.log.warning('Workers done!')
			msg = 'OK: {}, failed: {}, no filter: {} -- total: {}, fail rate: {}%, missing: {} '.format(numOkJobs,
			                       numFailedJobs,
			                       numNoFilter,
			                       (numOkJobs + numFailedJobs + numNoFilter),
			                       ((100 / (numOkJobs + numFailedJobs)) * numFailedJobs) if (
				                       (numOkJobs + numFailedJobs) > 0) else 0,
			                       len(objs) - (numOkJobs + numFailedJobs + numNoFilter))
			self.log.warning(msg)
			return msg

	def runFilterForWholeContainer(self):
		return self.runForWholeContainer(functionOnObject=self.getDataAndRunFilter)

	def runIdentifierForWholeContainer(self):
		return self.runForWholeContainer(functionOnObject=self.getDataAndIdentifyContentType)

	def runDummyLoad(self):
		return self.runForWholeContainer(functionOnObject=self.dummyLoad)
