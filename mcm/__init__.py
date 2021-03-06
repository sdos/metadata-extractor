#!/usr/bin/python
# coding=utf-8

"""
	Project MCM - Micro Content Management
	Metadata Extractor - identify content type, extract metadata with specific filter plugins


	Copyright (C) <2016> Tim Waizenegger, <University of Stuttgart>

	This software may be modified and distributed under the terms
	of the MIT license.  See the LICENSE file for details.
"""

import logging, coloredlogs, sys
from mcm.metadataExtractor import configuration


log_format = '%(asctime)s %(module)s %(name)s[%(process)d][%(thread)d] %(levelname)s %(message)s'
field_styles = {'module': {'color': 'magenta'}, 'hostname': {'color': 'magenta'}, 'programname': {'color': 'cyan'},
                'name': {'color': 'blue'}, 'levelname': {'color': 'black', 'bold': True}, 'asctime': {'color': 'green'}}

coloredlogs.install(level=configuration.log_level, fmt=log_format, field_styles=field_styles)

# very chatty module...
logging.getLogger("requests.packages.urllib3").setLevel(logging.ERROR)

logging.error("###############################################################################")
logging.error("Task runner service running")
logging.error("Python {}".format(sys.version))
logging.error("###############################################################################")