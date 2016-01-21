'''
Created on Jan 19, 2016

@author: tim
'''
from tim.metadataExtractor.ImportFilterInterface import ImportFilterInterface
import exifread


class ImportFilterJpeg(ImportFilterInterface):
	'''
	classdocs
	'''
	myName = 'jpeg'
	myContentType = 'image/jpeg'
	
	myValidTagNames = ['Image DateTime']

	def __init__(self):
		'''
		Constructor
		'''
	
	def extractMetaData(self, obj):
		tags = exifread.process_file(obj, details=False)
		#print(tags)
		return self.convertMetaDataToSwiftFormat(tags)	
	
	
	
	