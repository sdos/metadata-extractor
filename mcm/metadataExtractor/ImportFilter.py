#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Import Filters


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""

import logging

from mcm.metadataExtractor.ImportFilterDocuments import ImportFilterEmail
from mcm.metadataExtractor.ImportFilterDocuments import ImportFilterPDF
from mcm.metadataExtractor.ImportFilterImages import ImportFilterBmp
from mcm.metadataExtractor.ImportFilterImages import ImportFilterGif
from mcm.metadataExtractor.ImportFilterImages import ImportFilterJpeg
from mcm.metadataExtractor.ImportFilterImages import ImportFilterPng
from mcm.metadataExtractor.ImportFilterImages import ImportFilterTiff
from metadataExtractor.Exceptions import NoFilterFoundException

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
    logging.info("looking for filter type: >>{}<<".format(objType))

    try:
        return mapping[objType]()
    except KeyError:
        for name, filter in mapping.items():
            if objType.startswith(name):
                return filter()
    raise NoFilterFoundException("no filter for type: >{}<".format(objType))
