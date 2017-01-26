#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Metadata Extractor - identify content type, extract metadata with specific filter plugins


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""

from mcm.metadataExtractor.ImportFilterInterface import ImportFilterInterface
from email.parser import BytesParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser


class ImportFilterPDF(ImportFilterInterface):
    '''
    classdocs
    @author: Daniel Brühl
    '''
    myName = 'pdf'
    myContentType = 'application/pdf'
    myValidTagNames = [
        'title',
        'author',
        'creator',
        'creationdate',
        'producer',
        'moddate'
    ]

    def __init__(self):
        '''
        Constructor
        '''

    def extractMetaData(self, obj):
        tags = PDFDocument(PDFParser(obj)).info
        # print(str(tags[0]))
        return self.cleanupMetaDataDict(tags[0])


# email filter
class ImportFilterEmail(ImportFilterInterface):
    '''
    classdocs
    @author: Hoda Noori
    '''
    myName = 'email'
    myContentType = 'text/plain'

    myValidTagNames = ['content-transfer-encoding',
                       'to',
                       'from',
                       'subject',
                       'date',
                       'x-bcc',
                       'x-cc'
                       ]

    def __init__(self):
        '''
        Constructor
        '''

    def extractMetaData(self, obj):
        headers = BytesParser().parse(obj)
        metadata = dict(headers.items())

        return self.cleanupMetaDataDict(metadata)
