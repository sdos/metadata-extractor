'''
Created on Jan 19, 2016

@author: tim
'''
import logging


class ImportFilterInterface(object):
	'''
	classdocs
	'''
	log = logging.getLogger()  # logger for all import filters

	def __init__(self):
		'''
		Constructor
		'''

	def cleanupMetaDataDict(self, mdRaw):
		mdConv = dict()
		# print(mdRaw)
		for k, v in mdRaw.items():
			k_clean = k.replace(' ', '-').lower()
			if k_clean in self.myValidTagNames:
				if(bytes == type(v)):
					val = v.decode(encoding='UTF-8')
				else:
					val = v.__str__()
				mdConv['x-object-meta-filter-{}-{}'.format(self.myName, k_clean)] = val
		return mdConv

	def extractMetaData(self, obj):
		result = {'placeholder': 'placeholder'}
		print("TEST")
		return result
