'''
Created on Jan 19, 2016

@author: tim
'''
import logging
from tim.metadataExtractor.Extractor import Extractor



# log lvls: DEBUG - INFO - WARNING - ERROR - CRITICAL
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(module)s - %(levelname)s ##\t  %(message)s')

thisContainer = 'filter_test'

log = logging.getLogger()
ex = Extractor(containerName=thisContainer)




if __name__ == '__main__':
	log.error('starting metadata extractor tester')
	
	
	ex.runIdentifierForWholeContainer()
	ex.runFilterForWholeContainer()
	
	log.error('DONE')