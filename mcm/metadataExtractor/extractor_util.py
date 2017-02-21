#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Metadata Extractor - identify content type, extract metadata with specific filter plugins


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""


def msg_for_extractor(numFailedJobs, numNoFilter, numOkJobs, objs):
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


def msg_for_disposal(numFailedJobs, numNoRetentionDate, numRetentionInFuture, numOkJobs, objs):
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


def msg_for_identify(num_no_change, numFailedJobs, numOkJobs, objs):
    total = numOkJobs + numFailedJobs + num_no_change
    msg = 'Changed: {}, No change: {}, Failed: {} -- Total: {}, Fail rate: {}%, Missing: {} '.format(
        numOkJobs,
        num_no_change,
        numFailedJobs,
        total,
        (
            (
                100 / total) * numFailedJobs) if (
            total > 0) else 0,
        len(objs) - total)
    return msg


def msg_for_generic(numFailedJobs, numOkJobs, objs):
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
