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
		
		
	def convertMetaDataToSwiftFormat(self, mdRaw):
		mdConv = dict()
		#print(mdRaw)
		for k, v in mdRaw.items():
				mdConv['x-object-meta-filter_{}_{}'.format(self.myName, k.replace(' ', '_'))] = v.__str__()
		return mdConv	
		
	def extractMetaData(self, obj):
		result = {'placeholder':'placeholder'}
		print("TEST")
		return result
	
	
