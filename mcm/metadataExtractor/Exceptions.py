#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Metadata Extractor - identify content type, extract metadata with specific filter plugins


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""


class NoFilterFoundException(Exception):
	'''
	classdocs
	'''

	def __init__(self, message):
		'''
		Constructor
		'''
		self.message = message

class NoRetentionDateException(Exception):
	'''
	classdocs
	'''

	def __init__(self, message):
		'''
		Constructor
		'''
		self.message = message

class RetentionDateInFutureException(Exception):
	'''
	classdocs
	'''

	def __init__(self, message):
		'''
		Constructor
		'''
		self.message = message
