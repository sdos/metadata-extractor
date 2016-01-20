'''
Created on Jan 19, 2016

@author: tim
'''
import logging
from tim.swift.SwiftBackend import SwiftBackend
from tim.metadataExtractor.Extractor import Extractor




logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(module)s - %(levelname)s ##\t  %(message)s')

thisContainer = 'jpg-test'

log = logging.getLogger()
sb = SwiftBackend()
ex = Extractor(swiftBackend=sb, containerName=thisContainer)




if __name__ == '__main__':
	log.error('starting metadata extractor tester')
	
	ex.runForWholeContainer()
	
	
	log.error('DONE')