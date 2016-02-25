'''
Created on Feb 18, 2016

@author: Christoph Trybek
'''

from tim.metadataExtractor.ImportFilterInterface import ImportFilterInterface
from email.parser import BytesParser


# email filter
class ImportFilterEmail(ImportFilterInterface):
    '''
    classdocs
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