'''
Created on Feb 18, 2016
'''

from tim.metadataExtractor.ImportFilterInterface import ImportFilterInterface
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
		#print(str(tags[0]))
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