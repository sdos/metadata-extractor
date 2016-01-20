'''
Created on Jan 19, 2016

@author: tim
'''
import logging
from tim.metadataExtractor.ImportFilter import ImportFilterJpeg
from tim.swift.SwiftBackend import SwiftBackend
import swiftclient.multithreading
import concurrent.futures


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
		
	def getFilterForObjType(self, objType):
		return self.mapping[objType]()
	
		
	def getDataAndRunFilter(self, conn, objType, objName):
		thisObjBlob = self.sb.getObjBlob(conn, self.containerName, objName) 
		thisFilter = self.getFilterForObjType(objType)
		r = thisFilter.extractMetaData(thisObjBlob)
		return self.sb.writeObjMetaData(conn=conn, containerName=self.containerName, objName=objName, metaDict=r)
			
	def runForWholeContainer(self):
		with swiftclient.multithreading.ConnectionThreadPoolExecutor(self.sb._getConnection, max_workers=20) as executor:
			objs = self.sb.get_object_list(self.containerName)
			future_results = []
			
			# first go through all objs in the container and spawn a thread to run the filter
			for thisObj in objs:
				thisObjType = thisObj['content_type']
				thisObjName = thisObj['name']
				
				if (thisObjType not in self.mapping):
					self.log.error('no filter found for type : {} on obj: {}'.format(thisObjType, thisObjName))
					continue
				self.log.info('running filter for type : {} on obj: {}'.format(thisObjType, thisObjName))
				future_results.append(executor.submit(self.getDataAndRunFilter, thisObjType, thisObjName))
			
			# try to get the individual results from the filters
			for future in concurrent.futures.as_completed(future_results):
				try:
					data = future.result()
				except Exception as exc:
					self.log.error('Filter failed with exception: {}'.format(exc))
				else:
					self.log.info('Filter succeeded on obj: {}'.format(data))
				
			
			
