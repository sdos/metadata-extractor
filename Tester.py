'''
Created on Jan 19, 2016

@author: osecm
'''
import logging
from osecm.metadataExtractor.Extractor import Extractor

# log lvls: DEBUG - INFO - WARNING - ERROR - CRITICAL
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(module)s - %(levelname)s ##\t  %(message)s')

containerName = 'hallo'
swift_url = "http://192.168.209.204:8080/auth/v1.0"
swift_user = "test:tester"
swift_pw = "testing"

log = logging.getLogger()
ex = Extractor(containerName=containerName, swift_url=swift_url, swift_user=swift_user, swift_pw=swift_pw)

if __name__ == '__main__':
	log.error('starting metadata extractor tester')

	ex.runIdentifierForWholeContainer()
	#ex.runFilterForWholeContainer()
	#ex.runDummyLoad()

	log.error('DONE')
