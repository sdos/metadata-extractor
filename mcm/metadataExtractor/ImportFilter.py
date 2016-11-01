#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Import Filters


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""

from mcm.metadataExtractor.ImportFilterDocuments import ImportFilterEmail
from mcm.metadataExtractor.ImportFilterDocuments import ImportFilterPDF
from mcm.metadataExtractor.ImportFilterImages import ImportFilterBmp
from mcm.metadataExtractor.ImportFilterImages import ImportFilterGif
from mcm.metadataExtractor.ImportFilterImages import ImportFilterJpeg
from mcm.metadataExtractor.ImportFilterImages import ImportFilterPng
from mcm.metadataExtractor.ImportFilterImages import ImportFilterTiff

mapping = dict()
# image filters
mapping[ImportFilterBmp.myContentType] = ImportFilterBmp
mapping[ImportFilterGif.myContentType] = ImportFilterGif
mapping[ImportFilterJpeg.myContentType] = ImportFilterJpeg
mapping[ImportFilterPng.myContentType] = ImportFilterPng
mapping[ImportFilterTiff.myContentType] = ImportFilterTiff

# document filters
mapping[ImportFilterEmail.myContentType] = ImportFilterEmail
mapping[ImportFilterPDF.myContentType] = ImportFilterPDF


def getFilterForObjType(objType):
	return mapping[objType]()