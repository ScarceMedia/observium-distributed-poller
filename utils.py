'''
File: observium/crawler/utils.py
Author: John Chilton
Created: 9/15/2013
'''

import time, datetime, MySQLdb

def find_field(cursor, row, field):
	i = 0;
	for coldesc in cursor.description:
		if coldesc[0] == field:
			return row[i]
		i += 1
	return None

def now_in_sql_format():
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def datetime_from_str(datestr):
	return datetime.datetime.strptime(datestr, "%Y-%m-%d %H:%M:%S")