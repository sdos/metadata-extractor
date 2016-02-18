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
                mdConv['x-object-meta-filter-{}-{}'.format(self.myName, k_clean)] = v.__str__()
        return mdConv

    def extractMetaData(self, obj):
        result = {'placeholder': 'placeholder'}
        print("TEST")
        return result
