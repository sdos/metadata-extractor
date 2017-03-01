#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Metadata Extractor - identify content type, extract metadata with specific filter plugins


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""
import json
import logging
import os
import socket
from threading import Thread

from pykafka import KafkaClient
from pykafka.common import OffsetType

from mcm.metadataExtractor import configuration
from mcm.metadataExtractor.Extractor import Extractor

"""
Message definition
"""
valid_task_types = ["identify_content", "extract_metadata", "ping", "dispose", "replicate_metadata"]

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
        self.kc = KafkaClient(hosts=configuration.kafka_broker_endpoint, use_greenlets=False)
        # TODO: add support for listenting on multiple tenant queues
        self.topic = self.kc.topics[configuration.my_tenant_name.encode("utf-8")]

        consumer_group = 'mcmextractor-{}'.format(configuration.my_tenant_name).encode('utf-8')

        """
        we consume only NEW messages; set reset_offset_on_start=False to use gloabl offset.

        """
        # self.consumer = self.topic.get_balanced_consumer(managed=True,
        #                                                  consumer_group=consumer_group,
        #                                                  auto_commit_enable=True,
        #                                                  auto_offset_reset=OffsetType.LATEST,
        #                                                  reset_offset_on_start=True,
        #                                                  consumer_timeout_ms=-1)
        self.consumer = self.topic.get_simple_consumer(consumer_group=consumer_group,
                                                       use_rdkafka=False,
                                                       auto_commit_enable=True,
                                                       auto_offset_reset=OffsetType.LATEST,
                                                       reset_offset_on_start=True,
                                                       consumer_timeout_ms=-1,
                                                       fetch_min_bytes=1)

    def consumeMsgs(self):
        while True:
            msg = self.consumer.consume(block=True)
            if not msg:
                continue
            logging.debug("got msg: {}".format(msg))
            try:
                j = json.loads(msg.value.decode("utf-8"))
                if not j["type"] in valid_task_types:
                    logging.warning("msg type not ours: {}".format(j))
                    continue
                t = TaskRunner(j["tenant-id"], j["token"], j["type"], j["container"], j["correlation"], self.topic)
                t.start()
            except Exception:
                logging.exception("error consuming message: {}".format(msg.value))


class TaskRunner(Thread):
    '''
    gets instantiated in a new thread to process one message
    '''

    def __init__(self, tenant_id, token, type, container, correlation, topic):
        Thread.__init__(self)
        self.worker_id = "MCMTaskRunner-{}-{}".format(socket.getfqdn(), os.getpid())
        self.swift_url = configuration.swift_store_url_valid_prefix + tenant_id
        self.token = token
        self.task_type = type
        self.container = container
        self.correlation = correlation
        self.topic = topic

        logging.warning(
            "running task {} on container {} for tenant {} - corr: {}".format(type, container, tenant_id, correlation))

    def __send_ping(self):
        logging.info("pong")
        self.__notifySender("pong", task_type="pong")

    def __dispatch_task_type(self):
        if self.task_type in valid_task_types:
            ex = Extractor(container_name=self.container, storage_url=self.swift_url, token=self.token)
            if self.task_type == valid_task_types[0]:
                s = ex.runIdentifierForWholeContainer()
            elif self.task_type == valid_task_types[1]:
                s = ex.runFilterForWholeContainer()
            elif self.task_type == valid_task_types[3]:
                s = ex.runDisposalForWholeContainer()
            elif self.task_type == valid_task_types[4]:
                s = ex.runReplicateMetadataForAllContainers()
            self.__notifySender("task {} finished: {}".format(self.task_type, s), task_type="success")

    def run(self):
        if self.task_type == valid_task_types[2]:
            self.__send_ping()
            return
        m = 'starting task {}'.format(self.task_type, self.container)
        logging.info(m)
        self.__notifySender(m, task_type="processing")
        self.__dispatch_task_type()

    def __notifySender(self, msg, task_type="response"):
        j = {"type": task_type,
             "correlation": self.correlation,
             "container": self.container,
             "message": msg,
             "worker": self.worker_id}
        logging.info(j)
        with self.topic.get_producer(linger_ms=100) as producer:
            producer.produce(value_serializer(j))
