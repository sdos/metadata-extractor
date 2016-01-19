'''
Created on Jan 19, 2016

@author: tim
'''
import logging
from tim.metadataExtractor.ImportFilter import ImportFilterJpeg
import tim.metadataExtractor.ImportFilter



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
			
			
			
	def runForWholeContainer(self):
		objs = self.swiftBackend.get_object_list(self.containerName)
		for thisObj in objs:
			thisObjType = thisObj['content_type']
			thisObjName = thisObj['name']
			
			if (thisObjType not in self.mapping):
				self.log.error('no filter found for type : {} on obj: {}'.format(thisObjType, thisObjName))
				continue
			self.log.info('running filter for type : {} on obj: {}'.format(thisObjType, thisObjName))
			
			f = self.mapping[thisObjType]()
			thisObjBlob = self.swiftBackend.getObject(self.containerName, thisObjName)
			filterResult = f.extractMetaData(thisObjBlob)
			self.log.info('got filter result: {}'.format(filterResult))
			self.writeMetaData(objName=thisObjName, metaDict=filterResult)
			
			
			
			
	def writeMetaData(self, objName, metaDict):
		self.swiftBackend.updateObjectMetaData(container=self.containerName, name=objName, metaDict=metaDict)
	
			
			
			
			
			
			
			
			
			
			
			