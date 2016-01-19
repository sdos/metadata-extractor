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
	def convertMetaDataToSwiftFormat(self, mdRaw):
		mdConv = dict()
		print(mdRaw)
		for k, v in mdRaw.items():
			#if k in self.myValidTagNames:
				mdConv['x-object-meta-filter_{}_{}'.format(self.myName, k.replace(' ', '_'))] = v.__str__()
		return mdConv
	
	def extractMetaData(self, obj):
		tags = exifread.process_file(obj, details=False)
		return self.convertMetaDataToSwiftFormat(tags)	