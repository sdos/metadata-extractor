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
	
	myValidTagNames = [
			'image-datetime',
			'exif-lightsource',
			'image-gpsinfo',
			'exif-digitalzoomratio',
			'image-make',
			'image-yresolution',
			'image-model',
			'exif-exposuretime',
			'exif-exposuremode',
			'image-orientation',
			'exif-datetimeoriginal',
			'image-software',
			'exif-flash',
			'image-xresolution',
	]

	def __init__(self):
		'''
		Constructor
		'''
	
	def extractMetaData(self, obj):
		tags = exifread.process_file(obj, details=False)
		#print(tags)
		return self.cleanupMetaDataDict(tags)	
	
	
	
	