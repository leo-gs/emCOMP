'''
Usage python upload_flat_tweet_table.py db_config.txt json_files_dir
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

TABLE_NAME = "Timelines"
DROP_EXISTING_TABLES = True

db_config = sys.argv[1]
input_json_dir = sys.argv[2]

# Create table objects
tweet_table_fields = [
	Field("tweetId", "BIGINT", is_primary_key=True),
	Field("user_timeline_collected_at", "VARCHAR(64)"),
	Field("user_timeline_collected_ts", "TIMESTAMP"),
	Field("created_at", "VARCHAR(64)"),
	Field("created_ts", "TIMESTAMP"),
	Field("lang", "VARCHAR(20)"),
	Field("text", "VARCHAR(500)"),
	Field("contributors", "JSON"),
	Field("entities", "JSON"),
	Field("expanded_url", "VARCHAR(256)"),
	Field("filter_level", "VARCHAR(80)"),
	Field("coordinates", "JSON"),
	Field("place", "JSON"),
	Field("possibly_sensitive", "BOOLEAN"),
	Field("user", "JSON"),
	Field("user_id", "BIGINT"),
	Field("user_screen_name", "VARCHAR(140)"),
	Field("user_follower_count", "BIGINT"),
	Field("user_friends_count", "BIGINT"),
	Field("user_statuses_count", "BIGINT"),
	Field("user_favorites_count", "BIGINT"),
	Field("user_geo_enabled", "BOOLEAN"),
	Field("user_time_zone", "VARCHAR(100)"),
	Field("user_description", "VARCHAR(512)"),
	Field("user_location", "VARCHAR(512)"),
	Field("user_created_at", "VARCHAR(64)"),
	Field("user_created_ts", "TIMESTAMP"),
	Field("user_lang", "VARCHAR(8)"),
	Field("user_listed_count", "BIGINT"),
	Field("user_name", "VARCHAR(140)"),
	Field("user_url", "VARCHAR(512)"),
	Field("user_utc_offset", "BIGINT"),
	Field("user_verified", "BOOLEAN"),
	Field("user_profile_use_background_image", "BOOLEAN"),
	Field("user_default_profile_image", "BOOLEAN"),
        Field("user_profile_sidebar_fill_color", "VARCHAR(26)"),
	Field("user_profile_text_color", "VARCHAR(16)"),
	Field("user_profile_sidebar_border_color", "VARCHAR(16)"),
	Field("user_profile_background_color", "VARCHAR(16)"),
	Field("user_profile_link_color", "VARCHAR(16)"),
	Field("user_profile_image_url", "VARCHAR(256)"),
	Field("user_profile_banner_url", "VARCHAR(256)"),
	Field("user_profile_background_image_url", "VARCHAR(256)"),
	Field("user_profile_background_tile", "BOOLEAN"),
	Field("user_contributors_enabled", "BOOLEAN"),
	Field("user_default_profile", "BOOLEAN"),
	Field("user_id_translator", "BOOLEAN"),
	Field("retweet_count", "BIGINT"),
	Field("favorite_count", "BIGINT"),
	Field("retweeted_status", "JSON"),
	Field("retweeted_status_id", "BIGINT"),
	Field("retweeted_status_user_screen_name", "VARCHAR(80)"),
	Field("retweeted_status_retweet_count", "BIGINT"),
	Field("retweeted_status_user_id", "BIGINT"),
	Field("retweeted_status_user_time_zone", "VARCHAR(100)"),
	Field("retweeted_status_user_friends_count", "BIGINT"),
	Field("retweeted_status_user_statuses_count", "BIGINT"),
	Field("retweeted_status_user_followers_count", "BIGINT"),
	Field("source", "VARCHAR(500)"),
	Field("in_reply_to_screen_name", "VARCHAR(500)"),
	Field("in_reply_to_status_id", "BIGINT"),
	Field("in_reply_to_user_id", "BIGINT"),
	Field("quoted_status_id", "BIGINT"),
	Field("quoted_status", "JSON"),
	Field("truncated", "BOOLEAN")
]
tweet_table = Table(TABLE_NAME, tweet_table_fields)

def batch_insert(cursor, table, rows):
	fields_required = len(table.fields)
	for row in rows:
		assert len(row)==fields_required, "Row has incorrect number of fields: " + str(row)

	ext.execute_batch(cursor, table.get_insert_statement(), rows)

#####################################
#####################################

def clean(text):
	return text.replace("\x00", "") if text else None

def convert_timestring_to_timestamp(datetimestr):
        if datetimestr:
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
cursor = db.cursor()

## Creating tables in the database if they don't exist
if DROP_EXISTING_TABLES:
	cursor.execute(tweet_table.get_drop_statement(if_exists=True))

print(tweet_table.get_create_statement(if_not_exists=True))
cursor.execute(tweet_table.get_create_statement(if_not_exists=True))

inserted_count = 0
json_files = [f for f in os.listdir(input_json_dir) if (len(f) > 5 and f[-5:]==".json")]
for f in json_files:
	tweet_tuples = []

	json_data = json.load(open(os.path.join(input_json_dir, f)))
	tweets = json_data["historic_tweets"]

	collected_at = json_data["utc_timestamp"]
	collected_ts = convert_timestring_to_timestamp(collected_at)

	for tweet in tweets:
		tweet_tuple = (
		get_nested_value(tweet, "id"),
		collected_at,
		collected_ts,
		get_nested_value(tweet, "created_at"),
		convert_timestring_to_timestamp(get_nested_value(tweet, "created_at")),
		get_nested_value(tweet, "lang"),
		clean(get_nested_value(tweet, "text")),
		get_nested_value_json(tweet, "contributors"),
		get_nested_value_json(tweet, "entities"),
		get_nested_value(tweet, "entities.urls[0].expanded_url"),
		get_nested_value(tweet, "filter_level"),
		get_nested_value_json(tweet, "coordinates"),
		get_nested_value_json(tweet, "place"),
		get_nested_value(tweet, "possibly_sensitive"),
		get_nested_value_json(tweet, "user"),
		get_nested_value(tweet, "user.id"),
		get_nested_value(tweet, "user.screen_name"),
		get_nested_value(tweet, "user.followers_count"),
		get_nested_value(tweet, "user.friends_count"),
		get_nested_value(tweet, "user.statuses_count"),
		get_nested_value(tweet, "user.favourites_count"),
		get_nested_value(tweet, "user.geo_enabled"),
		get_nested_value(tweet, "user.time_zone"),
		get_nested_value(tweet, "user.description"),
		get_nested_value(tweet, "user.location"),
		get_nested_value(tweet, "user.created_at"),
		convert_timestring_to_timestamp(get_nested_value(tweet, "user.created_ts")),
		get_nested_value(tweet, "user.lang"),
		get_nested_value(tweet, "user.listed_count"),
		get_nested_value(tweet, "user.name"),
		get_nested_value(tweet, "user.url"),
		get_nested_value(tweet, "user.utc_offset"),
		get_nested_value(tweet, "user.verified"),
		get_nested_value(tweet, "user.profile_use_background_image"),
		get_nested_value(tweet, "user.default_profile_image"),
		get_nested_value(tweet, "user.profile_sidebar_fill_color"),
		get_nested_value(tweet, "user.profile_text_color"),
		get_nested_value(tweet, "user.profile_sidebar_border_color"),
		get_nested_value(tweet, "user.profile_background_color"),
		get_nested_value(tweet, "user.profile_link_color"),
		get_nested_value(tweet, "user.profile_image_url"),
		get_nested_value(tweet, "user.profile_banner_url"),
		get_nested_value(tweet, "user.profile_background_image_url"),
		get_nested_value(tweet, "user.profile_background_tile"),
		get_nested_value(tweet, "user.contributors_enabled"),
		get_nested_value(tweet, "user.default_profile"),
		get_nested_value(tweet, "user.is_translator"),
		get_nested_value(tweet, "retweet_count"),
		get_nested_value(tweet, "favorite_count"),
		get_nested_value_json(tweet, "retweeted_status"),
		get_nested_value(tweet, "retweeted_status.id"),
		get_nested_value(tweet, "retweeted_status.user.screen_name"),
		get_nested_value(tweet, "retweeted_status.retweet_count"),
		get_nested_value(tweet, "retweeted_status.user.id"),
		get_nested_value(tweet, "retweeted_status.user.time_zone"),
		get_nested_value(tweet, "retweeted_status.user.friends_count"),
		get_nested_value(tweet, "retweeted_status.user.statuses_count"),
		get_nested_value(tweet, "retweeted_status.user.followers_count"),
		get_nested_value(tweet, "source"),
		get_nested_value(tweet, "in_reply_to_screen_name"),
		get_nested_value(tweet, "in_reply_to_status_id"),
		get_nested_value(tweet, "in_reply_to_user_id"),
		get_nested_value(tweet, "quoted_status_id"),
		get_nested_value_json(tweet, "quoted_status"),
		get_nested_value(tweet, "truncated"))

                tweet_tuples.append(tweet_tuple)
                inserted_count = inserted_count + 1

	batch_insert(cursor, tweet_table, tweet_tuples)

	db.commit()
	inserted_count = inserted_count + len(tweet_tuples)
	print(str(inserted_count) + " total inserted")
	sys.stdout.flush() # so print statements get printed to logs more quickly

print("Done!")





