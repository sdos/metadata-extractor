'''
Created on Jan 19, 2016

@author: tim
'''
import logging
from tim.metadataExtractor.ImportFilter import ImportFilterJpeg
import tim.metadataExtractor.ImportFilter
import swiftclient.multithreading
import concurrent.futures
import io


class Extractor(object):
	'''
	classdocs
	'''
	mapping = dict()
	mapping[ImportFilterJpeg.myContentType] = ImportFilterJpeg

	def __init__(self, swiftBackend,containerName):
		'''
		Constructor
		'''
		self.log = logging.getLogger()
		self.swiftBackend = swiftBackend
		self.containerName = containerName
		self.log.info('initializing...')
	
	def writeMetaData(self, conn, objName, metaDict):
		self.log.debug('updating object metadata in swift. updating obj {} in container {}; adding {}'.format(objName, self.containerName, metaDict))
		conn.post_object(container=self.containerName, obj=objName, headers=metaDict, response_dict=None)
		
	def getDataAndRunFilter(self, conn, objType, objName):
		self.log.debug('running extractor on obj {} in container {}'.format(objName, self.containerName))
			
		thisObjBlob = io.BytesIO(conn.get_object(container=self.containerName, obj=objName, resp_chunk_size=None, query_string=None, response_dict=None, headers=None)[1])
		thisFilter = self.mapping[objType]()
		r = thisFilter.extractMetaData(thisObjBlob)
		self.writeMetaData(conn=conn, objName=objName, metaDict=r)
			
	def runForWholeContainer(self):
		objs = self.swiftBackend.get_object_list(self.containerName)
		with swiftclient.multithreading.ConnectionThreadPoolExecutor(self.swiftBackend._getConnection, 10) as executor:
			
			future_results = []
		
			for thisObj in objs:
				thisObjType = thisObj['content_type']
				thisObjName = thisObj['name']
				
				if (thisObjType not in self.mapping):
					self.log.error('no filter found for type : {} on obj: {}'.format(thisObjType, thisObjName))
					continue
				self.log.info('running filter for type : {} on obj: {}'.format(thisObjType, thisObjName))
				future_results.append(executor.submit(self.getDataAndRunFilter, thisObjType, thisObjName))
			
			for future in concurrent.futures.as_completed(future_results):
				try:
					data = future.result()
				except Exception as exc:
					print(exc)
				else:
					print('got result...')
				
			
			
