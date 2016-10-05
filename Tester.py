#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Metadata Extractor - identify content type, extract metadata with specific filter plugins


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""
import logging
from mcm.metadataExtractor.Extractor import Extractor

# log lvls: DEBUG - INFO - WARNING - ERROR - CRITICAL
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(module)s - %(levelname)s ##\t  %(message)s')

containerName = 'crypto-4'
#swift_url = "http://192.168.209.204:8080/auth/v1.0"
swift_url = "http://localhost:3000/auth/v1.0"
swift_user = "test:tester"
swift_pw = "testing"

log = logging.getLogger()
ex = Extractor(containerName=containerName, swift_url=swift_url, swift_user=swift_user, swift_pw=swift_pw)

if __name__ == '__main__':
	log.error('starting metadata extractor tester')

	ex.runIdentifierForWholeContainer()
	ex.runFilterForWholeContainer()
	# ex.runDummyLoad()

	log.error('DONE')
