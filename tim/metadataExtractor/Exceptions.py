'''
Created on Feb 11, 2016

@author: tim
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