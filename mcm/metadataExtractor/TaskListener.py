#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Metadata Extractor - identify content type, extract metadata with specific filter plugins


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""
import logging, json, os, socket
from threading import Thread

from pykafka.common import OffsetType

from mcm.metadataExtractor import configuration
from mcm.metadataExtractor.Extractor import Extractor
from pykafka import KafkaClient

"""
Message definition
"""
tt_0 = "identify_content"
tt_1 = "extract_metadata"
valid_task_types = {tt_0: "Identify content types",
                    tt_1: "Extract metadata"}

"""

Helpers
"""
value_serializer = lambda v: json.dumps(v).encode('utf-8')


def __try_parse_msg_content(m):
	try:
		return json.loads(m.value.decode("utf-8"))
	except Exception as e:
		return {"type": "Error", "error": "msg parsing failed"}


class Tasklistener(object):
	'''
	listen for messages on kafka and run the identifier/extractor
	'''

	def __init__(self):
		logging.warning("starting task listener")

		# without timeout the consumer will wait forever for new msgs
		self.kc = KafkaClient(hosts=configuration.kafka_broker_endpoint, use_greenlets=True)
		self.topic = self.kc.topics[configuration.swift_tenant.encode('utf-8')]

		consumer_group = 'mcmextractor-{}'.format(configuration.swift_tenant).encode('utf-8')
		consumer_id = 'mcmextractor-1'.encode('utf-8')

		"""
		we consume only NEW messages; set reset_offset_on_start=False to use gloabl offset.

		"""
		self.consumer = self.topic.get_balanced_consumer(zookeeper_connect=configuration.zookeeper_endpoint,
		                                                 consumer_group=consumer_group,
		                                               auto_commit_enable=True,
		                                               auto_offset_reset=OffsetType.LATEST,
		                                               reset_offset_on_start=True)

	def consumeMsgs(self):
		for m in self.consumer:
			logging.debug("got msg: {}".format(m))
			try:
				j = json.loads(m.value.decode("utf-8"))
				if not j["type"] in valid_task_types:
					logging.debug("msg type not ours")
					continue
				t = TaskRunner(configuration.swift_tenant, j["token"], j["type"], j["container"], j["correlation"])
				t.start()
			except Exception:
				self.consumer.stop()
				logging.exception("error consuming message")
		self.consumer.stop()

class TaskRunner(Thread):
	'''
	gets instantiated in a new thread to process one message
	'''

	def __init__(self, tenant, token, type, container, correlation):
		Thread.__init__(self)
		self.worker_id = "MCMTaskRunner-{}-{}".format(socket.getfqdn(), os.getpid())
		self.tenant = tenant
		self.token = token
		self.type = type
		self.container = container
		self.correlation = correlation
		# without timeout the consumer will wait forever for new msgs
		self.kc = KafkaClient(hosts=configuration.kafka_broker_endpoint, use_greenlets=True)
		self.topic = self.kc.topics[configuration.swift_tenant.encode('utf-8')]

		logging.info(
			"running task {} on container {} for tenant {} - corr: {}".format(type, container, tenant, correlation))

	def run(self):
		m = 'starting task {} on container {}'.format(self.type, self.container)
		logging.info(m)
		self.__notifySender(m)
		ex = Extractor(containerName=self.container, storage_url=configuration.swift_storage_url, token=self.token)
		if self.type == tt_0:
			s = ex.runIdentifierForWholeContainer()
			self.__notifySender("task {} finished: {}".format(tt_0, s), type="success")
		elif self.type == tt_1:
			s = ex.runFilterForWholeContainer()
			self.__notifySender("task {} finished: {}".format(tt_1, s), type="success")

	def __notifySender(self, msg, type="response"):
		j = {"type": type,
		     "correlation": self.correlation,
		     "message": msg,
		     "worker": self.worker_id}
		logging.info(j)
		with self.topic.get_producer() as producer:
			producer.produce(value_serializer(j))
