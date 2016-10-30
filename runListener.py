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

import time

from mcm.metadataExtractor.TaskListener import Tasklistener


log = logging.getLogger()

if __name__ == '__main__':

	while True:
		try:
			log.error('starting metadata extractor task listener')
			r = Tasklistener()
			r.consumeMsgs()
		except:
			log.exception("listener crashed, restarting in 5s")
			time.sleep(5)

