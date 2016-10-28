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
from mcm.metadataExtractor.TaskListener import Tasklistener


log = logging.getLogger()
r = Tasklistener()

if __name__ == '__main__':
	log.error('starting metadata extractor task listener')

	r.consumeMsgs()

	log.error('DONE')
