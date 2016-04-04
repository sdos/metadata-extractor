'''
Created on Feb 11, 2016

@author: osecm
'''

class NoFilterFoundException(Exception):
	'''
	classdocs
	'''


	def __init__(self, message):
		'''
		Constructor
		'''
		self.message = message