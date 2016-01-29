'''
Created on Jan 29, 2016

@author: tim
'''

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