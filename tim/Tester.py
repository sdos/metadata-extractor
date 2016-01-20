'''
Created on Jan 19, 2016

@author: tim
'''
import logging
from tim.metadataExtractor.Extractor import Extractor




logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(module)s - %(levelname)s ##\t  %(message)s')

thisContainer = 'themes'

log = logging.getLogger()
ex = Extractor(containerName=thisContainer)




if __name__ == '__main__':
	log.error('starting metadata extractor tester')
	
	ex.runForWholeContainer()
	
	
	log.error('DONE')