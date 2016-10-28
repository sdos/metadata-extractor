#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Metadata Extractor - identify content type, extract metadata with specific filter plugins


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""
import logging, json, time
from threading import Thread

from mcm.metadataExtractor import configuration
from kafka import KafkaConsumer, KafkaProducer

kafka_timeout = 10
"""
Message definition
"""
valid_task_types = {"identify_content": "Identify content types",
						"extract_metadata": "Extract metadata"}


"""
Helpers
"""

class Tasklistener(object):
	'''
	listen for messages on kafka and run the identifier/extractor
	'''
	def __init__(self):
		logging.warning("starting task listener")

		# without timeout the consumer will wait forever for new msgs
		self.c = KafkaConsumer(configuration.swift_tenant,
		                  bootstrap_servers=configuration.kafka_broker_endpoint,
		                  client_id='mcmextractor',
		                  group_id='mcmextractor-{}'.format(configuration.swift_tenant),
		                  enable_auto_commit=False)


	def consumeMsgs(self):
		for m in self.c:
			logging.info("got msg: {}".format(m))
			try:
				j = json.loads(m.value.decode("utf-8"))
				if not j["type"] in valid_task_types:
					logging.warning("msg type not ours")
					continue
				t = TaskRunner(configuration.swift_tenant, j["type"], j["container"], j["correlation"])
				t.start()
			except Exception:
				logging.exception("error consuming message")


class TaskRunner(Thread):
	'''
	gets instantiated in a new thread to process one message
	'''
	def __init__(self, tenant, type, container, correlation):
		Thread.__init__(self)
		self.tenant = tenant
		self.type = type
		self.container = container
		self.correlation = correlation
		self.kafka_producer = KafkaProducer(
			bootstrap_servers=configuration.kafka_broker_endpoint,
			value_serializer=lambda v: json.dumps(v).encode('utf-8'))

		logging.info(
			"running task {} on container {} for tenant {} - corr: {}".format(type, container, tenant, correlation))
		self.__notifySender("starting task {}".format(type))

	def run(self):
		logging.info("running task...")
		self.__notifySender("running task")


	def __notifySender(self, msg):
		j = {"type" : "response",
		     "correlation" : self.correlation,
		     "message" : msg}
		self.kafka_producer.send(self.tenant, j).get(timeout=kafka_timeout)

