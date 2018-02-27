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


def get_protected_users(api, uids):
	protected_users = {}
	
	## Pull user data for each user id from the Twitter apis
	for index in range(0, len(uids), 100):
		## Define a chunk of 100 user ids from the list (because the API only allows 100 users per request)
		chunk = uids[index : index+100]

		## Pass the chunk to the lookup methods and retrieve their data
		user_chunks = api.lookup_users(user_ids=chunk)

		## For each user returned (i.e. accounts that haven't been deleted), get whether the account is protected
		for user in user_chunks:
			protected_users[user.id] = user.protected

	## Return the dictionary
	return protected_users

## Get the list of user ids we want to look up
uids = get_input_list()

## Get an authenticated Twitter API object
api = authenticate()

## Pull the protected status of still-active accounts (users missing from this dictionary have been deleted)
users = get_protected_users(api, uids)

## Dump into a JSON file
json.dump(users, open("protected_users.json", "w+"))

## All the accounts in the dictionary are still active 
active_uids = json.load(open("protected_users.json")).keys()

## Write a csv with the active/deleted status of each account
with open("status_nodetable.csv", "w+") as f:
	writer = csv.writer(f)
	writer.writerow(["id", "status"])

	for uid in uids:
		status = "active" if uid in active_uids else "removed"
		writer.writerow([uid, status])





