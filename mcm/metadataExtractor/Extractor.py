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

from mcm.metadataExtractor import RetentionChecker, ImportFilter, Replicator, configuration
from mcm.metadataExtractor.ContentTypeIdentifier import ContentTypeIdentifier
from mcm.metadataExtractor.Exceptions import NoFilterFoundException, NoRetentionDateException, \
    RetentionDateInFutureException
from mcm.swift.SwiftBackend import SwiftBackend


class Extractor(object):
    '''
    classdocs
    '''

    def __init__(self, containerName, swift_url=None, swift_user=None, swift_pw=None, storage_url=None, token=None):
        '''
        Constructor
        '''
        self.containerName = containerName
        logging.info('initializing...')
        self.swift_user = swift_user
        if storage_url and token:
            self.sb = SwiftBackend(storage_url=storage_url, token=token)
        else:
            self.sb = SwiftBackend(swift_url=swift_url, swift_user=swift_user, swift_pw=swift_pw)
        self.numWorkers = 20

    def dummyLoad(self, conn, objType, objName):
        print(objName)
        thisObjBlob = self.sb.getObjBlob(conn, self.containerName, objName)

    def getDataAndIdentifyContentType(self, conn, objType, objName):
        thisObjBlob = self.sb.getObjBlob(conn, self.containerName, objName)
        ctype = ContentTypeIdentifier().identifyContentType(thisObjBlob)
        if objType == ctype:
            return "same same..."
        return self.sb.updateObjContentType(conn, containerName=self.containerName, objName=objName,
                                            newContentType=ctype)

    def getDataAndRunFilter(self, conn, objType, objName):
        thisObjBlob = self.sb.getObjBlob(conn, self.containerName, objName)
        try:
            thisFilter = ImportFilter.getFilterForObjType(objType)
        except Exception as e:
            raise NoFilterFoundException("{} - {} - {}".format(objName, objType, e))
        r = thisFilter.extractMetaData(thisObjBlob)
        return self.sb.updateMetaDataFields(conn=conn, containerName=self.containerName, objName=objName, metaDict=r)

    def getMetadataAndRunDisposal(self, conn, objType, objName):
        return RetentionChecker.checkRetentionDate(conn=conn, containerName=self.containerName, objectName=objName)

    def getMetadataAndReplicate(self, conn, objType, objName):
        postgreConn = self.postgreConnPool.getconn()
        try:
            Replicator.replicateMetadata(conn=conn, containerName=self.containerName, objectName=objName,
                                         objectType=objType, postgreConn=postgreConn)
        finally:
            self.postgreConnPool.putconn(postgreConn)

    def runForWholeContainer(self, functionOnObject):
        with swiftclient.multithreading.ConnectionThreadPoolExecutor(self.sb._getConnection,
                                                                     max_workers=self.numWorkers) as executor:
            objs = self.sb.get_object_list(self.containerName)
            future_results = []

            # first go through all objs in the container and spawn a thread to run the filter
            logging.error('committing {} jobs for {}'.format(len(objs), functionOnObject.__name__))
            for thisObj in objs:

                try:
                    thisObjType = thisObj.get('content_type')
                    thisObjName = thisObj['name']

                    logging.info('running {} for type: {} on obj: {}'.format(functionOnObject.__name__, thisObjType,
                                                                             thisObjName))
                    future_results.append(executor.submit(functionOnObject, thisObjType, thisObjName))
                except Exception as exc:
                    logging.exception('could not create job for obj: {})'.format(thisObj))

            # try to get the individual results from the filters
            logging.error('Starting {} worker threads...'.format(self.numWorkers))
            numFailedJobs = 0
            numNoFilter = 0
            numNoRetentionDate = 0
            numRetentionInFuture = 0
            numOkJobs = 0
            for future in concurrent.futures.as_completed(future_results):
                try:
                    data = future.result()
                except NoFilterFoundException as exc:
                    logging.info('no filter found: {}'.format(exc))
                    numNoFilter += 1
                except NoRetentionDateException as exc:
                    logging.info('no retention date on obj: {}'.format(exc))
                    numNoRetentionDate += 1
                except RetentionDateInFutureException as exc:
                    logging.info('retention date in future on obj: {}'.format(exc))
                    numRetentionInFuture += 1
                except Exception as exc:
                    logging.exception('worker failed with exception')
                    numFailedJobs += 1
                else:
                    numOkJobs += 1
                    logging.info('worker succeeded on obj: {}'.format(data))
            logging.warning('Workers done!')

            if functionOnObject == self.getDataAndRunFilter:
                msg = self.__msg_for_extractor(numFailedJobs, numNoFilter, numOkJobs, objs)
            elif functionOnObject == self.getMetadataAndRunDisposal:
                msg = self.__msg_for_disposal(numFailedJobs, numNoRetentionDate, numRetentionInFuture, numOkJobs, objs)
            else:
                msg = self.__msg_for_generic(numFailedJobs, numOkJobs, objs)
            logging.warning(msg)
            return msg

    def __msg_for_extractor(self, numFailedJobs, numNoFilter, numOkJobs, objs):
        total = numOkJobs + numFailedJobs + numNoFilter
        msg = 'OK: {}, Failed: {}, No filter: {} -- Total: {}, Fail rate: {}%, Missing: {} '.format(
            numOkJobs,
            numFailedJobs,
            numNoFilter,
            total,
            ((100 / (
                numOkJobs + numFailedJobs)) * numFailedJobs) if (
                (
                    numOkJobs + numFailedJobs) > 0) else 0,
            len(objs) - total)
        return msg

    def __msg_for_disposal(self, numFailedJobs, numNoRetentionDate, numRetentionInFuture, numOkJobs, objs):
        total = numOkJobs + numRetentionInFuture + numNoRetentionDate + numFailedJobs
        msg = 'Deleted: {}, No retention: {}, Retention in future: {}, Failed: {} -- Total: {}, Fail rate: {}%, Missing: {} '.format(
            numOkJobs,
            numNoRetentionDate,
            numRetentionInFuture,
            numFailedJobs,
            total,
            ((100 / total) * numFailedJobs) if (
                total > 0) else 0,
            len(objs) - total)
        return msg

    def __msg_for_generic(self, numFailedJobs, numOkJobs, objs):
        total = numOkJobs + numFailedJobs
        msg = 'OK: {}, Failed: {} -- Total: {}, Fail rate: {}%, Missing: {} '.format(
            numOkJobs,
            numFailedJobs,
            total,
            (
                (
                    100 / total) * numFailedJobs) if (
                total > 0) else 0,
            len(objs) - total)
        return msg

    def runFilterForWholeContainer(self):
        return self.runForWholeContainer(functionOnObject=self.getDataAndRunFilter)

    def runIdentifierForWholeContainer(self):
        return self.runForWholeContainer(functionOnObject=self.getDataAndIdentifyContentType)

    def runDisposalForWholeContainer(self):
        return self.runForWholeContainer(functionOnObject=self.getMetadataAndRunDisposal)

    def runReplicateMetadataForWholeContainer(self):

        # create connection pool
        self.postgreConnPool = psycopg2.pool.ThreadedConnectionPool(5, self.numWorkers, None,
                                                                    **configuration.metadata_warehouse_endpoint)
        # create tables if not exist
        postgreConn = self.postgreConnPool.getconn()
        try:
            Replicator.createTablesIfAbsent(postgreConn)
        finally:
            self.postgreConnPool.putconn(postgreConn)

        return self.runForWholeContainer(functionOnObject=self.getMetadataAndReplicate)

    def runDummyLoad(self):
        return self.runForWholeContainer(functionOnObject=self.dummyLoad)
