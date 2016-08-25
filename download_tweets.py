# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 11:32:02 2016

@author: SAMERA
"""

import json, tweepy

consumer_key = "15Rln5atfHoeT"
consumer_secret = "C9CB3SlZ2ZxGqhw3WKjniZluyCQFXbWWd"
access_key = "743159365381787648-cviHOGnkiKx5DW5PkloSbi"
access_secret = "f6vLHwcF5LJTKbAGoJWhYSAkOnF3m26QepTQ"

start_line = 0

def get_tweet_id(line):
    '''
    Extracts and returns tweet ID from a line in the input.
    '''
    tw = line.strip().split()[0]
    #line = str(line).strip()
    
    #(tagid,_timestamp,_sandyflag) = line.split('\t')
    #(_tag, _search, tweet_id) = tagid.split(':')
    return tw

def get_tweets_single(twapi, idfilepath):
    '''
    Fetches content for tweet IDs in a file one at a time,
    which means a ton of HTTPS requests, so NOT recommended.

    `twapi`: Initialized, authorized API object from Tweepy
    `idfilepath`: Path to file containing IDs
    '''
    # process IDs from the file
    with open(idfilepath, 'rb') as idfile:
        for line in idfile:
            tweet_id = get_tweet_id(line)
            if tweet_id == '':
                continue 
            
            print('Fetching tweet for ID %s', tweet_id)
            try:
                tweet = twapi.get_status(tweet_id)
                print('%s,%s' % (tweet_id, tweet.text.encode('UTF-8')))
            except tweepy.TweepError as te:
                print('Failed to get tweet ID %s: %s', tweet_id, te.message)
                # traceback.print_exc(file=sys.stderr)
        # for
    # with

def get_tweet_list(twapi, idlist):
    '''
    Invokes bulk lookup method.
    Raises an exception if rate limit is exceeded.
    '''
    # fetch as little metadata as possible
    tweets = twapi.statuses_lookup(id_=idlist, include_entities=True, trim_user=False)
    
    return tweets
    #for tweet in tweets:
    #    print('%s,%s' % (tweet.id, tweet.text.encode('UTF-8')))

def get_tweets_bulk(twapi, idlist, dbcollection):
    if len(idlist)==0:
        return 
        
    bulk = dbcollection.initialize_unordered_bulk_op()
    tweets = get_tweet_list(twapi, idlist)
    
    for t in idlist:
        t = int(t)
        bulk.find({'_id': t}).update({'$set': {'status': 'Error'}})
        
    for t in tweets:
        #j = json.dumps(t._json)
        j = t._json
        tid = j['id']
        bulk.find({'_id': tid}).update({'$set': {'json': j, 'status': 'Loaded'}})
        #bulk.find({'_id': tid}).update({'$set': {'status': 'Loaded'}})
    
    bulk.execute()
    return len(tweets)
#%%
import tweepy
    
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

#%%
from datetime import datetime
from pymongo import MongoClient
import time, pymongo

client = MongoClient()
client = MongoClient("mongodb://localhost:27017")

db = client.events2012
dbcoll = db.posts


cTotal = 0
while True:
    cursor = dbcoll.find({'status':"New"}).sort([('_id', pymongo.ASCENDING)]).limit(100)
    idlist = []
    for c in cursor:
        idlist.append( c['_id'])
        
    if len(idlist) == 0:
        break
    
    try:
        c = get_tweets_bulk(api, idlist, dbcoll)
        cLoaded = dbcoll.find({'status': 'Loaded'}).count()
        cErrors = dbcoll.find({'status': 'Error'}).count()
        print('Updated in db - success :', cLoaded, '. Error: ', cErrors)
    except Exception as e:
        sec = 0.25*60
        current_time = datetime.now().time() 
        print ('[', current_time.isoformat(), '] Going to sleep ', sec/60.0, ' minutes!\nException : ', e)
        time.sleep(sec)
        print ('Waking up!')
        #raise

print ("Finished!")

#%%

reset = False
if reset:
    dbcoll.update_many({'status': 'Error'}, {'$set': {'status': 'New'}}) #find({'status': 'Error'})
#for e in errors:
#    e.update

