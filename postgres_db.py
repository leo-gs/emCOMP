'''
Usage python postres_db.py db_config.txt json_files_dir
'''

import datetime
import json
import os
import psycopg2
from sql_utils import Field, Table
import sys

TABLE_PREFIX = "Harvey"
DROP_EXISTING_TABLES = True
INCLUDE_PARENT_TWEETS = False

# Create table objects
tweet_table_fields = [
	Field("tweetId", "BIGINT", is_primary_key=True),
	Field("truncated", "BOOLEAN"),
	Field("isQuoteStatus", "BOOLEAN"),
	Field("inReplyToStatusId", "BIGINT"),
	Field("favoriteCount", "BIGINT"),
	Field("source", "TEXT"),
	Field("coordinates_x", "NUMERIC(16,8)"),
	Field("coordinates_y", "NUMERIC(16,8)"),
	Field("inReplyToScreenName", "VARCHAR(45)"),
	Field("retweetCount", "BIGINT"),
	Field("inReplyToUserId", "BIGINT"),
	Field("lang", "VARCHAR(10)"),
	Field("createdAt", "TIMESTAMP"),
	Field("collectedAt", "TIMESTAMP")
]
tweet_table = Table("Tweet", tweet_table_fields, prefix=TABLE_PREFIX)
tweet_foreign_key = tweet_table.get_field("tweetId")

tweetuser_table_fields = [
	Field("tweetId", "BIGINT", is_primary_key=True, foreign_key=tweet_foreign_key, foreign_key_table=tweet_table),
	Field("userId", "BIGINT"),
	Field("timeZone", "VARCHAR(20)"),
	Field("verified", "BOOLEAN"),
	Field("geoEnabled", "BOOLEAN"),
	Field("followersCount", "BIGINT"),
	Field("protected", "BOOLEAN"),
	Field("lang", "VARCHAR(10)"),
	Field("utcOffset", "BIGINT"),
	Field("statusesCount", "BIGINT"),
	Field("description", "TEXT"),
	Field("friendsCount", "BIGINT"),
	Field("name", "VARCHAR(128)"),
	Field("favoritesCount", "BIGINT"),
	Field("screenName", "VARCHAR(45)"),
	Field("url", "TEXT"),
	Field("createdAt", "TIMESTAMP"),
	Field("location", "VARCHAR(256)"),
	Field("collectedAt", "TIMESTAMP")
]
tweetuser_table = Table("TweetUser", tweetuser_table_fields, prefix=TABLE_PREFIX)

tweethashtag_table_fields = [
	Field("tweetId", "BIGINT", is_primary_key=True, foreign_key=tweet_foreign_key, foreign_key_table=tweet_table),
	Field("hashtag", "TEXT")
]
tweethashtag_table = Table("TweetHashtag", tweethashtag_table_fields, prefix=TABLE_PREFIX)

tweetmention_table_fields = [
	Field("tweetId", "BIGINT", is_primary_key=True, foreign_key=tweet_foreign_key, foreign_key_table=tweet_table),
	Field("userId", "BIGINT"),
	Field("mentionedId", "BIGINT"),
	Field("mentionedScreenName", "VARCHAR(45)"),
	Field("mentionedName", "VARCHAR(128)")
]
tweetmention_table = Table("TweetMention", tweetmention_table_fields, prefix=TABLE_PREFIX)

tweeturl_table_fields = [
	Field("tweetId", "BIGINT", is_primary_key=True, foreign_key=tweet_foreign_key, foreign_key_table=tweet_table),
	Field("url", "TEXT"),
	Field("display_url", "TEXT"),
	Field("expanded_url", "TEXT")
]
tweeturl_table = Table("TweetUrl", tweeturl_table_fields, prefix=TABLE_PREFIX)

tweetplace_table_fields = [
	Field("tweetId", "BIGINT", is_primary_key=True, foreign_key=tweet_foreign_key, foreign_key_table=tweet_table),
	Field("boundingBoxJson", "TEXT"),
	Field("country", "VARCHAR(128)"),
	Field("countryCode", "VARCHAR(10)"),
	Field("fullName", "TEXT"),
	Field("placeId", "BIGINT"),
	Field("placeName", "VARCHAR(128)"),
	Field("placeUrl", "TEXT")
]
tweetplace_table = Table("TweetPlace", tweetplace_table_fields, prefix=TABLE_PREFIX)

all_tables = [tweet_table, tweetuser_table, tweethashtag_table, tweetmention_table, tweeturl_table, tweetplace_table]

def batch_insert(cursor, table, rows):
	fields_required = len(table.fields)
	for row in rows:
		assert len(row)==fields_required, "Row has incorrect number of fields: " + str(row)

	psycopg2.extras.execute_batch(cursor, table.get_insert_statement(), rows)

#####################################
#####################################

def convert_timestring_to_timestamp(datetimestr):
    datetime = parser.parse(datetimestr)
    return datetime.strftime('%Y-%m-%d %H:%M:%S')

def get_tweet_tuple(tweet, collectedAt):
	tweetId = tweet["id_str"]
	truncated = tweet["truncated"]
	isQuoteStatus = tweet["is_quote_status"]
	inReplyToStatusId = tweet["in_reply_to_status_id"]
	favoriteCount = tweet["favorite_count"]
	source = tweet["source"]
	coordinates_x, coordinates_y = None, None
	if "coordinates" in tweet and tweet["coordinates"] is not None:
		coordinates_x = tweet["coordinates"]["coordinates"][0]
		coordinates_y = tweet["coordinates"]["coordinates"][1]
	inReplyToScreenName = tweet["in_reply_to_screen_name"]
	retweetCount = tweet["retweet_count"]
	inReplyToUserId = tweet["in_reply_to_user_id"]
	tweet_lang = tweet["lang"]
	tweet_createdAt = convert_timestring_to_timestamp(tweet["created_at"])

	return (tweetId, truncated, isQuoteStatus, inReplyToStatusId, favoriteCount, source, coordinates_x, coordinates_y, inReplyToScreenName, retweetCount, inReplyToUserId, tweet_lang, tweet_createdAt, collectedAt)

def get_tweetuser_tuple(tweet, collectedAt):
	tweetId = tweet["id_str"]
	tweetuser = tweet["user"]

	userId = tweetuser["id_str"]
	timeZone = tweetuser["time_zone"]
	verified = tweetuser["verified"]
	geoEnabled = tweetuser["geo_enabled"]
	followersCount = tweetuser["followers_count"]
	protected = tweetuser["protected"]
	user_lang = tweetuser["lang"]
	utcOffset = tweetuser["utc_offset"]
	statusesCount = tweetuser["statuses_count"]
	description = tweetuser["description"]
	friendsCount = tweetuser["friends_count"]
	name = tweetuser["name"]
	favoritesCount = tweetuser["favourites_count"]
	screenName = tweetuser["screen_name"]
	url = tweetuser["url"]
	user_createdAt = tweetuser["created_at"]
	location = tweetuser["location"]

	return (tweetId, userId, timeZone, verified, geoEnabled, followersCount, protected, user_lang, utcOffset, statusesCount, description, friendsCount, name, favoritesCount, screenName, url, user_createdAt, location, collectedAt)

def get_tweetplace_tuple(tweet, collectedAt):
	tweetplace = tweet.get("place", None)

	if tweetplace:
		boundingBoxJson = tweetplace["bounding_box"]
		country = tweetplace["country"]
		countryCode = tweetplace["country_code"]
		fullName = tweetplace["full_name"]
		placeId = tweetplace["id"]
		placeName = tweetplace["name"]
		placeUrl = tweetplace["url"]

		return (tweetId, boundingBoxJson, country, countryCode, fullName, placeId, placeName, placeUrl)


#####################################
#####################################

## Connecting to the database
config = {}
for line in open(sys.argv[1]).readlines():
	key, value = line.strip().split("=")
	config[key] = value
db = psycopg2.connect(**config)
cursor = db.cursor()

## Creating tables in the database if they don't exist
for table in all_tables:
	if DROP_EXISTING_TABLES:
		cursor.execute(table.get_drop_statement(if_exists=True))
	cursor.execute(table.get_create_statement(if_not_exists=True))

input_json_dir = sys.argv[2]
json_files = [f for f in os.listdir(input_json_dir) if (len(f) > 5 and f[-5:]==".json")]
for f in json_files:
	json_data = json.load(os.path.join(input_json_dir, f))

	tweets = json_data["historic_tweets"]
	collectedAt = convert_timestring_to_timestamp(tweet_json["utc_timestamp"])

	tweet_tuples = []
	tweetuser_tuples = []
	tweetplace_tuples = []
	tweethashtag_tuples = []
	tweetmention_tuples = []
	tweeturl_tuples = []

	for tweet in tweets:
		tweetId = tweet["id_str"]

		tweet_tuples.append(get_tweet_tuple(tweet, collectedAt))
		tweetuser_tuples.append(get_tweetuser_tuple(tweet, collectedAt))

		if INCLUDE_PARENT_TWEETS:
			if "retweeted_status" in tweet and tweet["retweeted_status"] is not None:
				retweet = tweet["retweeted_status"]
				# do some processing here

			if "quoted_status" in tweet and tweet["quoted_status"] is not None:
				quoted_tweet = tweet["quoted_status"]
				# do some processing here

		tweetplace = get_tweetplace_tuple(tweet, collectedAt)

		entities = tweet.get("entities", None)
		if entities:
			if "hashtags" in entities:
				for hashtag in entities["hashtags"]:
					tweethashtag_tuples.append((tweetId, hashtag["text"]))

			if "user_mentions" in entities:
				for mention in entities["user_mentions"]:
					mentionedId = mention["id_str"]
					mentionedScreenName = mention["screen_name"]
					mentionedName = mention["name"]

					tweetmention_tuples.append((tweetId, userId, mentionedId, mentionedScreenName, mentionedName))

			if "urls" in entities:
				for url in entities["urls"]:
					entity_url = url["url"]
					display_url = url["display_url"]
					expanded_url = url["expanded_url"]

					tweeturl_tuples.append((tweetId, entity_url, display_url))

	batch_insert(cursor, tweet_table, tweet_tuples)
	batch_insert(cursor, tweetuser_table, tweeturl_table)
	batch_insert(cursor, tweetplace_table, tweetplace_tuples)
	batch_insert(cursor, tweethashtag_table, tweethashtag_tuples)
	batch_insert(cursor, tweetmention_table, tweetmention_tuples)
	batch_insert(cursor, tweeturl_table, tweeturl_tuples)

	db.commit()







