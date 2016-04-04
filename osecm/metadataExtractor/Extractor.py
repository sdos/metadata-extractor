'''
Created on Jan 19, 2016

@author: osecm
'''
import logging
from osecm.metadataExtractor.ImportFilterImages import ImportFilterBmp
from osecm.metadataExtractor.ImportFilterImages import ImportFilterGif
from osecm.metadataExtractor.ImportFilterImages import ImportFilterJpeg
from osecm.metadataExtractor.ImportFilterImages import ImportFilterPng
from osecm.metadataExtractor.ImportFilterImages import ImportFilterTiff
from osecm.metadataExtractor.ImportFilterDocuments import ImportFilterEmail
from osecm.swift.SwiftBackend import SwiftBackend
import swiftclient.multithreading
import concurrent.futures
from osecm.metadataExtractor.ContentTypeIdentifier import ContentTypeIdentifier
from osecm.metadataExtractor.Exceptions import NoFilterFoundException
from osecm.metadataExtractor.ImportFilterDocuments import ImportFilterPDF


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

	def __init__(self, containerName, swift_url, swift_user, swift_pw):
		'''
		Constructor
		'''
		self.log = logging.getLogger()
		self.containerName = containerName
		self.log.info('initializing...')
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

					self.log.info('running {} for type : {} on obj: {}'.format(functionOnObject.__name__, thisObjType,
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
					self.log.info('worker succeeded on obj: {}'.format(data))
			self.log.error('Workers done!')
			self.log.error('OK: {}, failed: {}, no filter: {} -- total: {}, fail rate: {}%, missing: {} '
						   .format(numOkJobs,
								   numFailedJobs,
								   numNoFilter,
								   (numOkJobs + numFailedJobs + numNoFilter),
								   ((100 / (numOkJobs + numFailedJobs)) * numFailedJobs) if (
									   (numOkJobs + numFailedJobs) > 0) else 0,
								   len(objs) - (numOkJobs + numFailedJobs + numNoFilter)))

	def runFilterForWholeContainer(self):
		self.runForWholeContainer(functionOnObject=self.getDataAndRunFilter)

	def runIdentifierForWholeContainer(self):
		self.runForWholeContainer(functionOnObject=self.getDataAndIdentifyContentType)
		
	def runDummyLoad(self):
		self.runForWholeContainer(functionOnObject=self.dummyLoad)
