import csv
import datetime
import json
import os
import sys
import time

import tweepy
from tweepy.auth import OAuthHandler

'''
Collects the k most recent tweets given a list of users and stores the tweets as JSON files.

Before using, set the value of CAP to be k and make sure all_uids contains a list of user ids.

Usage: python get_historic_tweets_from_date.py
'''


FROM_DATE_STR = "Fri Aug 18 00:00:00 +0000 2017"

def convert_str_to_datetime(datetime_str):
	date_format = "%a %b %d %H:%M:%S +0000 %Y"
	return datetime.datetime.strptime(datetime_str, date_format)

def get_now():
	return datetime.datetime.utcnow()

from_date = convert_str_to_datetime(FROM_DATE_STR)

def authenticate():
	## Pulling twitter login credentials from "config" file
	## The file should have the consumer key, consumer secret, access token, and access token secret in that order, separated by newlines.
	config = open("config/twitter_config_1.txt").read().split()
	consumer_key = config[0]
	consumer_secret = config[1]
	access_token = config[2]
	access_token_secret = config[3]

	## Authenticating
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)

	## Returning the authenticated API object
	api = tweepy.API(auth)
	return api

## Gets 3200 of the most recent tweets associated with the given uid before before_id
## (or the 3200 most recent tweets if before_id is None)
## Returns the minimum id of the list of tweets (i.e. the id corresponding to the earliest tweet)
def get_historic_tweets_before_id(api, uid, max_id=None):
	## Printing out the user id (for debugging)
	print(uid)

	## List of tweets we've collected so far
	tweets = []

	## The timeline is returned as pages of tweets (each page has 20 tweets, starting with the 20 most recent)
	## If a cap has been set and our list of tweets gets to be longer than the cap, we'll stop collecting
	cursor_args = {"id": uid, "count": 200}
	if max_id:
		cursor_args["max_id"] = max_id

	try:
		for page in tweepy.Cursor(api.user_timeline, **cursor_args).pages(16):
			## Adding the tweets to the list
			tweets.extend([tweet._json for tweet in page])

			## We get 900 requests per 15-minute window, or 1 request/second, so wait 1 second between each request just to be safe
			time.sleep(1)
	
	except tweepy.RateLimitError:
		## We received a rate limiting error, so wait 15 minutes
		time.sleep(15*60)

		## Try again
		tweets = get_historic_tweets_before_id(api, uid, max_id)

	if tweets:
		max_id, oldest_tweet_date = tweets[0]['id'], convert_str_to_datetime(tweets[0]['created_at'])
		for tweet in tweets[1:]:
			print(max_id)
			print(oldest_tweet_date)
			print('')
			if tweet['id'] < max_id:
				max_id = tweet['id']
				oldest_tweet_date = convert_str_to_datetime(tweet['created_at'])
		return (max_id, oldest_tweet_date, tweets)

## Get a uid's tweets since FROM_DATE
def get_historic_tweets(api, uid):
	max_id, oldest_tweet_date = None, get_now()

	tweets = []

	while oldest_tweet_date > from_date:
		request_result = get_historic_tweets_before_id(api, uid, max_id)
		if request_result:
			max_id, oldest_tweet_date, returned_tweets = request_result
			print("1. " + str(len(returned_tweets)) + " total returned")

			if oldest_tweet_date < from_date:
				returned_tweets = [tweet for tweet in returned_tweets if convert_str_to_datetime(tweet['created_at']) >= from_date]
				print("2. " + str(len(returned_tweets)) + " after filtering")

			tweets.extend(returned_tweets)
		else:
			break

	return tweets




""""""
""""""

## Get an authenticated API object
api = authenticate()

## Load list of uids to collect
all_uids = []
with open("harvey_ids_2.csv") as f:
	reader = csv.reader(f)
	for row in reader:
		all_uids.append(row[0])

## Get a list of uids we've already collected by seeing which JSON files we have (so we don't collect on the same users twice)
completed_uids = set([fname.split('.')[0] for fname in os.listdir("json_data_2")])


## Figure out which uids are left
uids_remaining = set(all_uids) - completed_uids
print(len(uids_remaining))

## Loop through the list of remaining uids					
for uid in uids_remaining:
	utc_now = str(get_now()) ## Get the timestamp of when we collected the tweets and convert it to a string so it can be stored in JSON

	try:
		## Pull the tweets using Tweepy
		historic_tweets = get_historic_tweets(api, uid)
		print(str(len(historic_tweets)) + " tweets collected")

		if historic_tweets:
			## Add the uid and the timestamp to the JSON
			data = {"user_id":uid, "utc_timestamp":utc_now, "historic_tweets":historic_tweets}

			## Dump the JSON into a file with the name <uid>.json
			with open("json_data_2/" + str(uid) + ".json", "w+") as data_file:
				json.dump(data, data_file)

			## Print out how many tweets we've collected per user id (for debugging)
			print(str(uid) + ': ' + str(len(historic_tweets)) + ' tweets collected')

	## If we get a Tweepy error, print the uid and error and keep running
	except tweepy.error.TweepError as ex:
		print(uid)
		print(ex)
