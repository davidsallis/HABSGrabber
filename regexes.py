#---------------------------------------------------------------------------------------------
#
# regexes.py
#   Regular expressions used in processing incoming HABS data.
#
#---------------------------------------------------------------------------------------------

import re

unsigned_number_regex = re.compile(r'\d+\.?\d*')
signed_number_regex = re.compile(r'[\+?|\-?]?\d+\.?\d*')
token_regex = re.compile(r'\S+')
parenthetical_regex = re.compile(r'\(.*\)')
