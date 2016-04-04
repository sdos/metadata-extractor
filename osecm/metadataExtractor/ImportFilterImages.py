'''
Created on Feb 18, 2016

@author: Christoph Trybek
'''
from osecm.metadataExtractor.ImportFilterInterface import ImportFilterInterface
import exifread
from PIL import Image

class ImportFilterBmp(ImportFilterInterface):
	'''
	classdocs
	@author: Christoph Trybek
	'''
	myName = 'bmp'
	myContentType = 'image/x-ms-bmp'

	myValidTagNames = [
		'image-size',
		'dpi',
		'compression'
	]

	def __init__(self):
		'''
		Constructor
		'''

	def extractMetaData(self, obj):
		imgfile = Image.open(obj)
		metadata = imgfile.info
		metadata['image-size'] = str(imgfile.size[0]) + " x " + str(imgfile.size[1])

		return self.cleanupMetaDataDict(metadata)


class ImportFilterGif(ImportFilterInterface):
	'''
	classdocs
	@author: Christoph Trybek
	'''
	myName = 'gif'
	myContentType = 'image/gif'

	myValidTagNames = [
		'image-size',
		'background',
		'duration'
	]

	def __init__(self):
		'''
		Constructor
		'''

	def extractMetaData(self, obj):
		imgfile = Image.open(obj)
		metadata = imgfile.info
		metadata['image-size'] = str(imgfile.size[0]) + " x " + str(imgfile.size[1])

		return self.cleanupMetaDataDict(metadata)


class ImportFilterJpeg(ImportFilterInterface):
	'''
	classdocs
	@author: Christoph Trybek
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
		'image-size'
	]

	def __init__(self):
		'''
		Constructor
		'''

	def extractMetaData(self, obj):
		metadata = exifread.process_file(obj, details=False)
		imgfile = Image.open(obj)
		metadata['image-size'] = str(imgfile.size[0]) + " x " + str(imgfile.size[1])

		if not metadata:
			imgfile = Image.open(obj)
			metadata['image-size'] = str(imgfile.size[0]) + " x " + str(imgfile.size[1])

		return self.cleanupMetaDataDict(metadata)


class ImportFilterPng(ImportFilterInterface):
	'''
	classdocs
	@author: Christoph Trybek
	'''
	myName = 'png'
	myContentType = 'image/png'

	myValidTagNames = [
		'image-size'
	]

	def __init__(self):
		'''
		Constructor
		'''

	def extractMetaData(self, obj):
		metadata = {}

		imgfile = Image.open(obj)
		metadata['image-size'] = str(imgfile.size[0]) + " x " + str(imgfile.size[1])

		return self.cleanupMetaDataDict(metadata)


class ImportFilterTiff(ImportFilterInterface):
	'''
	classdocs
	@author: Christoph Trybek
	'''
	myName = 'tiff'
	myContentType = 'image/tiff'

	myValidTagNames = [
		'image-size',
		'image-rowsperstrip',
		'image-predictor',
		'image-photometricinterpretation',
		'image-extrasamples',
		'image-stripoffsets',
		'image-orientation',
		'image-intercolorprofile',
		'image-planarconfiguration',
		'image-sampleformat',
		'image-samplesperpixel',
		'image-bitspersample',
		'image-stripbytecounts',
		'image-compression'
	]

	def __init__(self):
		'''
		Constructor
		'''

	def extractMetaData(self, obj):
		metadata = exifread.process_file(obj, details=False)
		imgfile = Image.open(obj)
		metadata['image-size'] = str(imgfile.size[0]) + " x " + str(imgfile.size[1])

		if not metadata:
			imgfile = Image.open(obj)
			metadata['image-size'] = str(imgfile.size[0]) + " x " + str(imgfile.size[1])

		return self.cleanupMetaDataDict(metadata)
