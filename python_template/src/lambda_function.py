import handler
import json
import os

from python_template.src.DataService import DataService


#
# AWS Lambda Functions Default Function
#
# This hander is used as a bridge to call the platform neutral
# version in handler.py. This script is put into the scr directory
# when using publish.sh.
#
# @param request
#
def lambda_handler(event, context):
	return DataService.handle_request(event, context)
	