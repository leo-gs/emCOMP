import csv
import datetime
import json
import os
import sys
import time

import tweepy
from tweepy.auth import OAuthHandler

'''
Usage: python get_active_accounts.py input_list.csv twitter_config.txt
'''

def get_input_list():
	uids = []
	with open(sys.argv[1]) as f:
		reader = csv.reader(f)
		reader.next()

		for row in reader:
			uid = row[0]
			uids.append(uid)

	return uids

def authenticate():
	## Pulling twitter login credentials from "config" file
	## The file should have the consumer key, consumer secret, access token, and access token secret in that order, separated by newlines.
	config = open(sys.argv[2]).read().split()
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

def get_user_status(uid):
	try:
		user_data = api.get_user(user_id=uid)
		return "protected" if user_data.protected else "active"

	except tweepy.TweepError as e:
		if(e.api_code==63):
			return "suspended"
		if(e.api_code==50):
			return "not found"
		if(e.api_code==179):
			return "protected"

def get_user_statuses(api, uids):
	user_statuses = {}
	
	## Pull user data for each user id from the Twitter apis
	for index in range(0, len(uids), 100):
		## Define a chunk of 100 user ids from the list (because the API only allows 100 users per request)
		chunk = uids[index : index+100]

		for uid in chunk:
			user_status = get_user_status(uid)
			user_statuses[uid] = user_status
		print(index)
		time.sleep(300)

	## Return the dictionary
	return user_statuses

## Get the list of user ids we want to look up
uids = get_input_list()

## Get an authenticated Twitter API object
api = authenticate()

## Pull the protected status of still-active accounts (users missing from this dictionary have been deleted)
user_statuses = get_user_statuses(api, uids)

## Write a csv with the account statuses
with open("status_nodetable.csv", "w+") as f:
	writer = csv.writer(f)
	writer.writerow(["id", "status"])

	for uid, status in user_statuses.items():
		writer.writerow([uid, status])





