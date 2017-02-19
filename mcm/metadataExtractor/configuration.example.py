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
kafka_broker_endpoint = "172.18.0.33:9092"

swift_store_url_valid_prefix = "http://172.18.0.11:3000/v1/AUTH_"

#my_tenant_id = "ea012720129645d9b32b23b4af5c154f"
my_tenant_id = "mcmdemo"

"""
################################################################################
used by Analytics. Endpoint of the metadata warehouse PostgreSQL db
################################################################################
"""
metadata_warehouse_endpoint = {
	"database": "mcm_metadata_{}".format(my_tenant_id),
	"user": "postgres",
	"password": "passw0rd",
	"host": "172.18.0.44",
	"port": "5432"
}