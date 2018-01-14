'''
Usage python upload_followers.py db_config.txt json_files_dir
'''

import datetime
from dateutil import parser
import json
import os
import psycopg2
from psycopg2 import extras as ext
from sql_utils import Field, Table
import sys
import time

TABLE_NAME = "Followers"
DROP_EXISTING_TABLES = False

db_config = sys.argv[1]
input_json_dir = sys.argv[2]

# Create table objects
follower_fields = [
	Field("user_id", "BIGINT"),
	Field("follower_id", "BIGINT"),
]
followers_table = Table(TABLE_NAME, follower_fields)

metadata_fields = [
	Field("user_id", "BIGINT"),
	Field("user_timeline_collected_at", "VARCHAR(64)"),
	Field("user_timeline_collected_ts", "TIMESTAMP"),
	Field("followers_collected", "BIGINT")
]
metadata_table = Table(TABLE_NAME + "_metadata", metadata_fields)

def batch_insert(cursor, table, rows):
	fields_required = len(table.fields)
	for row in rows:
		assert len(row)==fields_required, "Row has incorrect number of fields: " + str(row)

	ext.execute_batch(cursor, table.get_insert_statement(), rows)

#####################################
#####################################

def convert_timestring_to_timestamp(datetimestr):
	datetime = parser.parse(datetimestr)
	return datetime.strftime('%Y-%m-%d %H:%M:%S')

def get_nested_value(_dict, path, default=None):
	""" gets value from a nested value """
	# step through each path and try to process it
	parts = path.split(".")
	num_parts = len(parts)
	cur_dict = _dict
	# step through each part of the path
	try:
		for i in range(0, num_parts - 1):
			part = parts[i]
			if part[0] >= ord('0') and part[0] <= ord('9'):
				try:
					part = int(part)
				except ValueError:
					pass
			cur_dict = cur_dict[part]
		return cur_dict[parts[num_parts - 1]]
	except (KeyError, TypeError):
		pass
	return default

def get_nested_value_json(_dict, path, default=None):
		value = get_nested_value(_dict, path, default)
		if value is not None:
				return json.dumps(value)
		return value

## Connecting to the database
config = {}
for line in open(db_config).readlines():
	key, value = line.strip().split("=")
	config[key] = value
db = psycopg2.connect(**config)
time.sleep(5)
cursor = db.cursor()

## Creating tables in the database if they don't exist
if DROP_EXISTING_TABLES:
	cursor.execute(followers_table.get_drop_statement(if_exists=True))
	cursor.execute(metadata_table.get_drop_statement(if_exists=TRUE))
cursor.execute(followers_table.get_create_statement(if_not_exists=True))
cursor.execute(metadata_table.get_create_statement(if_not_exists=True))

inserted_count = 0
json_files = [f for f in os.listdir(input_json_dir) if (len(f) > 5 and f[-5:]==".json")]
for f in json_files:

	json_data = json.load(open(os.path.join(input_json_dir, f)))

	user_id = json_data["user_id"]

	followers = json_data["followers"]
        follower_tuples = [(user_id, follower_id) for follower_id in followers]

	collected_at = json_data["utc_timestamp"]
	collected_ts = convert_timestring_to_timestamp(collected_at)

	followers_collected_count = len(followers)

	batch_insert(cursor, followers_table, follower_tuples)
	batch_insert(cursor, metadata_table, [(user_id, collected_at, collected_ts, followers_collected_count)])

	db.commit()
	inserted_count = inserted_count + 1
	print(str(inserted_count) + " total inserted")
	sys.stdout.flush() # so print statements get printed to logs more quickly







