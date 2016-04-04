'''
Created on Jan 19, 2016

@author: tim
'''
import logging
from tim.metadataExtractor.Extractor import Extractor

# log lvls: DEBUG - INFO - WARNING - ERROR - CRITICAL
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(module)s - %(levelname)s ##\t  %(message)s')

thisContainer = 'paper'

log = logging.getLogger()
ex = Extractor(containerName=thisContainer)

if __name__ == '__main__':
	log.error('starting metadata extractor tester')

	ex.runIdentifierForWholeContainer()
	ex.runFilterForWholeContainer()
	#ex.runDummyLoad()

	log.error('DONE')
