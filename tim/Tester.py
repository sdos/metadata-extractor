'''
Created on Jan 19, 2016

@author: tim
'''
import logging
from tim.metadataExtractor.Extractor import Extractor




logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(module)s - %(levelname)s ##\t  %(message)s')

thisContainer = 'enron-email_1'

log = logging.getLogger()
ex = Extractor(containerName=thisContainer)




if __name__ == '__main__':
	log.error('starting metadata extractor tester')
	
	
	ex.runIdentifierForWholeContainer()
	ex.runFilterForWholeContainer()
	
	log.error('DONE')