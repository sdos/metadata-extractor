#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Metadata Extractor - identify content type, extract metadata with specific filter plugins


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""

import magic


class ContentTypeIdentifier(object):
	'''
	classdocs
	'''

	def __init__(self):
		'''
		Constructor
		'''

	def identifyContentType(self, obj):
		return magic.from_buffer(obj.getvalue(), mime=True).decode(encoding='utf-8', errors='strict')
