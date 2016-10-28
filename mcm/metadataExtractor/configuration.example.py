#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Metadata Extractor


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.





	This is the configuration file
"""

import logging, math, os


###############################################################################
"""
	Log level setting
"""
#log_level = logging.CRITICAL
#log_level = logging.ERROR
#log_level = logging.WARNING
#log_level = logging.INFO
log_level = logging.DEBUG
log_format = '%(asctime)s - %(module)s - %(levelname)s ##\t  %(message)s'

###############################################################################
"""
	API / service settings
"""
swift_tenant = "test"
swift_auth_url = "http://192.168.209.204:8080/auth/v1.0"
swift_storage_url = "http://192.168.209.204:8080/v1/AUTH_{}".format(swift_tenant)

kafka_broker_endpoint = "192.168.209.208:9092"