'''
Created on Jan 19, 2016

@author: tim
'''
import logging
from tim.metadataExtractor.ImportFilter import ImportFilterJpeg
from tim.swift.SwiftBackend import SwiftBackend
import swiftclient.multithreading
import concurrent.futures
from tim.metadataExtractor.ContentTypeIdentifier import ContentTypeIdentifier


class Extractor(object):
	'''
	classdocs
	'''
	mapping = dict()
	mapping[ImportFilterJpeg.myContentType] = ImportFilterJpeg

	def __init__(self, containerName):
		'''
		Constructor
		'''
		self.log = logging.getLogger()
		self.containerName = containerName
		self.log.info('initializing...')
		self.sb = SwiftBackend()
		self.numWorkers = 20
		
	def getFilterForObjType(self, objType):
		return self.mapping[objType]()
	
	
	
	def getDataAndIdentifyContentType(self, conn, objType, objName):
		thisObjBlob = self.sb.getObjBlob(conn, self.containerName, objName)
		ctype = ContentTypeIdentifier().identifyContentType(thisObjBlob)
		if objType == ctype:
			return "same same..."
		return self.sb.updateObjContentType(conn, containerName=self.containerName, objName=objName, newContentType=ctype)
		
		
		
	def getDataAndRunFilter(self, conn, objType, objName):
		thisObjBlob = self.sb.getObjBlob(conn, self.containerName, objName)
		try:
			thisFilter = self.getFilterForObjType(objType)
		except:
			raise TypeError("No Filter for type {}".format(objType))
		r = thisFilter.extractMetaData(thisObjBlob)
		return self.sb.writeObjMetaData(conn=conn, containerName=self.containerName, objName=objName, metaDict=r)
			
			
			
	def runForWholeContainer(self, functionOnObject):
		with swiftclient.multithreading.ConnectionThreadPoolExecutor(self.sb._getConnection, max_workers=self.numWorkers) as executor:
			objs = self.sb.get_object_list(self.containerName)
			future_results = []
			
			# first go through all objs in the container and spawn a thread to run the filter
			self.log.error('committing {} jobs for {}'.format(len(objs), functionOnObject.__name__))
			for thisObj in objs:
				thisObjType = thisObj['content_type']
				thisObjName = thisObj['name']
				
				self.log.info('running {} for type : {} on obj: {}'.format(functionOnObject.__name__, thisObjType, thisObjName))
				future_results.append(executor.submit(functionOnObject, thisObjType, thisObjName))
			
			# try to get the individual results from the filters
			self.log.error('Starting {} worker threads...'.format(self.numWorkers))
			numFailedJobs = 0
			numOkJobs = 0
			for future in concurrent.futures.as_completed(future_results):
				try:
					data = future.result()
				except Exception as exc:
					self.log.warning('worker failed with exception: {}'.format(exc))
					numFailedJobs += 1
				else:
					numOkJobs += 1
					self.log.info('worker succeeded on obj: {}'.format(data))
			self.log.error('Workers done!')
			self.log.error('OK: {}, failed: {}, total: {}, fail rate: {}, missing: {} '
						.format(numOkJobs, 
							numFailedJobs, 
							(numOkJobs + numFailedJobs),
							(100/(numOkJobs + numFailedJobs)) * numFailedJobs,
							len(objs) - (numOkJobs + numFailedJobs)))
	
	
	def runFilterForWholeContainer(self):
			self.runForWholeContainer(functionOnObject=self.getDataAndRunFilter)
			
			
			
	def runIdentifierForWholeContainer(self):
			self.runForWholeContainer(functionOnObject=self.getDataAndIdentifyContentType)
			
			
