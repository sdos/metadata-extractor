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
					'Image_DateTime',
					'EXIF_LightSource',
					'Image_GPSInfo',
					'EXIF_DigitalZoomRatio',
					'Image_Make',
					'Image_YResolution',
					'Image_Model',
					'EXIF_ExposureTime',
					'EXIF_ExposureMode',
					'Image_Orientation',
					'EXIF_DateTimeOriginal',
					'Image_Software',
					'EXIF_Flash',
					'Image_XResolution'
	]

	def __init__(self):
		'''
		Constructor
		'''
	
	def extractMetaData(self, obj):
		tags = exifread.process_file(obj, details=False)
		#print(tags)
		return self.cleanupMetaDataDict(tags)	
	
	
	
	