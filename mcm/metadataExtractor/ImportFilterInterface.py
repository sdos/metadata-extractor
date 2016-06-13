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
import urllib


class ImportFilterInterface(object):
	'''
	classdocs
	'''
	log = logging.getLogger()  # logger for all import filters

	def __init__(self):
		'''
		Constructor
		'''

	def cleanupMetaDataDict(self, mdRaw):
		mdConv = dict()
		# print(mdRaw)
		for k, v in mdRaw.items():
			k_clean = k.replace(' ', '-').lower()
			if k_clean in self.myValidTagNames:
				if (bytes == type(v)):
					val = v.decode(encoding='UTF-8')
				else:
					val = v.__str__()
				val = urllib.parse.quote(val)
				mdConv['x-object-meta-filter-{}-{}'.format(self.myName, k_clean)] = val
		return mdConv

	def extractMetaData(self, obj):
		result = {'placeholder': 'placeholder'}
		print("TEST")
		return result
