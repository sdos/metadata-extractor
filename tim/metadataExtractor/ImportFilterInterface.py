'''
Created on Jan 19, 2016

@author: tim
'''
import logging

class ImportFilterInterface(object):
	'''
	classdocs
	'''
	log = logging.getLogger() # logger for all import filters
	
	def __init__(self):
		'''
		Constructor
		'''
		
		
	def extractMetaData(self, obj):
		result = {'placeholder':'placeholder'}
		return result
	
	
