#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Metadata Extractor - identify content type, extract metadata with specific filter plugins


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""
import concurrent.futures
import logging

import psycopg2.pool
import swiftclient.multithreading

from mcm.metadataExtractor import RetentionChecker, ImportFilter, Replicator, configuration, extractor_util
from mcm.metadataExtractor.ContentTypeIdentifier import ContentTypeIdentifier
from metadataExtractor.Exceptions import *
from mcm.swift.SwiftBackend import SwiftBackend


class Extractor(object):
    '''
    classdocs
    '''

    def __init__(self, container_name, swift_url=None, swift_user=None, swift_pw=None, storage_url=None, token=None):
        '''
        Constructor
        '''
        self.container_name = container_name
        logging.info('initializing...')
        self.swift_user = swift_user
        if storage_url and token:
            self.sb = SwiftBackend(storage_url=storage_url, token=token)
        else:
            self.sb = SwiftBackend(swift_url=swift_url, swift_user=swift_user, swift_pw=swift_pw)
        self.num_workers = 20

    def run_for_all_objects_in_container(self, function_on_object):
        """
        this is the main executor method
        start worker threads with specific extract/idetify/... functions on a selected container
        :param function_on_object:
        :return:
        """
        with swiftclient.multithreading.ConnectionThreadPoolExecutor(self.sb._getConnection,
                                                                     max_workers=self.num_workers) as executor:
            all_objects = self.sb.get_object_list(self.container_name)
            future_results = []

            # first go through all objs in the container and spawn a thread to run the filter
            logging.error('committing {} jobs for {}'.format(len(all_objects), function_on_object.__name__))
            for current_object in all_objects:

                try:
                    current_obj_type = current_object.get('content_type')
                    current_obj_name = current_object['name']

                    logging.info(
                        'running {} for type: {} on obj: {}'.format(function_on_object.__name__, current_obj_type,
                                                                    current_obj_name))
                    future_results.append(executor.submit(function_on_object, current_obj_type, current_obj_name))
                except Exception as e:
                    logging.exception('could not create job for obj: {})'.format(current_object))

            # try to get the individual results from the filters
            logging.error('Starting {} worker threads...'.format(self.num_workers))
            num_jobs_failed = 0
            num_jobs_no_filter = 0
            num_jobs_no_retention = 0
            num_jobs_retention_in_future = 0
            num_jobs_unchanged_type = 0
            num_jobs_ok = 0
            for results in concurrent.futures.as_completed(future_results):
                try:
                    data = results.result()
                except NoFilterFoundException as e:
                    logging.info('no filter found: {}'.format(e))
                    num_jobs_no_filter += 1
                except NoRetentionDateException as e:
                    logging.info('no retention date on obj: {}'.format(e))
                    num_jobs_no_retention += 1
                except RetentionDateInFutureException as e:
                    logging.info('retention date in future on obj: {}'.format(e))
                    num_jobs_retention_in_future += 1
                except SameTypeException as e:
                    logging.info('{}'.format(e))
                    num_jobs_unchanged_type += 1
                except Exception as e:
                    logging.exception('worker failed with exception')
                    num_jobs_failed += 1
                else:
                    num_jobs_ok += 1
                    logging.info('worker succeeded on obj: {}'.format(data))
            logging.warning('Workers done!')

            if function_on_object == self.getDataAndRunFilter:
                msg = extractor_util.msg_for_extractor(num_jobs_failed, num_jobs_no_filter, num_jobs_ok, all_objects)
            elif function_on_object == self.getMetadataAndRunDisposal:
                msg = extractor_util.msg_for_disposal(num_jobs_failed, num_jobs_no_retention,
                                                        num_jobs_retention_in_future,
                                                        num_jobs_ok, all_objects)
            elif function_on_object == self.getDataAndIdentifyContentType:
                msg = extractor_util.msg_for_identify(num_jobs_unchanged_type, num_jobs_failed, num_jobs_ok,
                                                        all_objects)
            else:
                msg = extractor_util.msg_for_generic(num_jobs_failed, num_jobs_ok, all_objects)
            logging.warning(msg)
            return msg

    """
    ################################################################################
    Task Implementation
    extract metadata / run filter
    ################################################################################
    """

    def runFilterForWholeContainer(self):
        return self.run_for_all_objects_in_container(function_on_object=self.getDataAndRunFilter)

    def getDataAndRunFilter(self, conn, objType, objName):
        thisObjBlob = self.sb.getObjBlob(conn, self.container_name, objName)
        thisFilter = ImportFilter.getFilterForObjType(objType)
        r = thisFilter.extractMetaData(thisObjBlob)
        return self.sb.updateMetaDataFields(conn=conn, containerName=self.container_name, objName=objName,
                                            metaDict=r)

    """
    ################################################################################
    Task Implementation
    identify content types
    ################################################################################
    """

    def runIdentifierForWholeContainer(self):
        return self.run_for_all_objects_in_container(function_on_object=self.getDataAndIdentifyContentType)

    def getDataAndIdentifyContentType(self, conn, objType, objName):
        thisObjBlob = self.sb.getObjBlob(conn, self.container_name, objName)
        ctype = ContentTypeIdentifier().identifyContentType(thisObjBlob)
        if objType == ctype:
            raise SameTypeException(
                "Object {} already has type {}".format(objName, objType))
        return self.sb.updateObjContentType(conn, containerName=self.container_name, objName=objName,
                                            newContentType=ctype)

    """
    ################################################################################
    Task Implementation
    dispose old objects
    ################################################################################
    """

    def runDisposalForWholeContainer(self):
        return self.run_for_all_objects_in_container(function_on_object=self.getMetadataAndRunDisposal)

    def getMetadataAndRunDisposal(self, conn, objType, objName):
        return RetentionChecker.checkRetentionDate(conn=conn, containerName=self.container_name, objectName=objName)

    """
    ################################################################################
    Task Implementation
    replicate metadata to database
    ################################################################################
    """

    def runReplicateMetadataForAllContainers(self):

        # create connection pool
        self.postgreConnPool = psycopg2.pool.ThreadedConnectionPool(5, self.num_workers, None,
                                                                    **configuration.metadata_warehouse_endpoint)
        # create tables if not exist
        postgreConn = self.postgreConnPool.getconn()
        try:
            Replicator.createTablesIfAbsent(postgreConn)
        finally:
            self.postgreConnPool.putconn(postgreConn)

        containers = self.sb.get_container_list()
        Replicator.replicate_container_info(postgres_connection=postgreConn, container_info=containers)

        messages = []

        for this_container in containers:
            self.container_name = this_container["name"]
            logging.info("replicating container: {}".format(self.container_name))
            msg = self.run_for_all_objects_in_container(function_on_object=self.getMetadataAndReplicate)
            messages.append("container: '{}': {}     ".format(self.container_name, msg))


        return messages

    def getMetadataAndReplicate(self, conn, objType, objName):
        postgreConn = self.postgreConnPool.getconn()
        try:
            Replicator.replicateMetadata(conn=conn, containerName=self.container_name, objectName=objName,
                                         objectType=objType, postgreConn=postgreConn)
        finally:
            self.postgreConnPool.putconn(postgreConn)

    """
    ################################################################################
    Task Implementation
    dummy load
    ################################################################################
    """

    def runDummyLoad(self):
        return self.run_for_all_objects_in_container(function_on_object=self.dummyLoad)

    def dummyLoad(self, conn, objType, objName):
        print(objName)
        thisObjBlob = self.sb.getObjBlob(conn, self.container_name, objName)
