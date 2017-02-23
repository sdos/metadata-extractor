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

import logging, os


###############################################################################
"""
	Log level setting
"""
#log_level = logging.CRITICAL
#log_level = logging.ERROR
log_level = logging.WARNING
#log_level = logging.INFO
#log_level = logging.DEBUG

###############################################################################

"""
################################################################################
MCM tenant
 this extractor is always responsible for one tenant. Use multiple extractor
 instances for multiple tenants
################################################################################
"""

my_tenant_name = os.getenv("TENANT_NAME", "mcmdemo")



"""
################################################################################
define the Swift server connection below:
localhost:3000 is the default for the SDOS API proxy.
here we assume that auth/store run on the same host/port. this is true with SDOS
 as well as swift-all-in-one setups.
 If bluebox is directly connected to Swift (without SDOS proxy) then you may need
 to configure two different endpoints below
################################################################################
"""

swift_host = os.getenv("SWIFT_HOST", "localhost")
swift_port = os.getenv("SWIFT_PORT", 3000)

swift_store_url_valid_prefix = "http://{}:{}/v1/AUTH_".format(swift_host, swift_port)

"""
################################################################################
Kafka bootstrap broker for messaging
################################################################################
"""
kafka_host = os.getenv("KAFKA_HOST", "localhost")
kafka_port = os.getenv("KAFKA_PORT", 9092)

kafka_broker_endpoint = "{}:{}".format(kafka_host, kafka_port)

"""
################################################################################
used by Analytics. Endpoint of the metadata warehouse PostgreSQL db
################################################################################
"""
postgres_host = os.getenv("POSTGRES_HOST", "localhost")
postgres_port = os.getenv("POSTGRES_PORT", 5432)

metadata_warehouse_endpoint = {
    "database": "mcm_metadata_{}".format(my_tenant_name),
    "user": "postgres",
    "password": "passw0rd",
    "host": postgres_host,
    "port": postgres_port
}
