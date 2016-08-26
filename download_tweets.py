# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 11:32:02 2016

@author: SAMERA
"""

#%%

#%%

import configparser

CONF_INI_FILE = 'c:/temp/conf.ini'

#conf.ini should look like this (in c:/temp folder)
#[DEFAULT]
#consumer_key = <key>
#consumer_secret = <secret>
#access_key = <key>
#access_secret = <secret>
#
#; default is localhost:27017 for mongodb
#mongodb_host = localhost
#mongodb_port = 27017

def load_config(user='DEFAULT'):
    config = configparser.ConfigParser()
    config.read(CONF_INI_FILE)
    
    
    default = config['DEFAULT']
    host = default['mongodb_host']
    port = int ( default['mongodb_port']  )
    
    default = config[user]
    consumer_key = default['consumer_key']
    consumer_secret = default['consumer_secret']
    access_key = default['access_key']
    access_secret = default['access_secret']

    
    return consumer_key, consumer_secret, access_key, access_secret, host, port

#%%


import tweepy

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
   
switch=0
errors = [0]*len(USERS)
apis = [None]*len(USERS)


consumer_key, consumer_secret, access_key, access_secret, host, port =load_config(USERS[switch])

if apis[switch] == None:
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    #
    apis[switch] = tweepy.API(auth)
    #apis[switch] = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

#%%
from pymongo import MongoClient
import time, pymongo
from tweepy import TweepError, RateLimitError

client = MongoClient(host, int(port))
#client = MongoClient("mongodb://"+host+":"+port)

db = client.events2012
dbcoll = db.posts


while True:
    idlist = []
    for c in cursor:
        idlist.append( c['_id'])
        
    if len(idlist) == 0:
        break
    
    try:
        c = get_tweets_bulk(apis[switch], idlist, dbcoll)
        cLoaded = dbcoll.find({'status': 'Loaded'}).count()
        cErrors = dbcoll.find({'status': 'Error'}).count()
        print('Updated in db - success :', cLoaded, '. Error: ', cErrors)
        
    except (RateLimitError , TweepError ) as e:
        
        if (type(e) == TweepError and str(e)[-3:] == '429')         \
            or isinstance(e, RateLimitError)      \
           :
            errors[switch] = time.time()
            if abs(errors[-1] - errors[0]) < 2:
                print('Too much failures... go to sleep! ', time.ctime())
                time.sleep(60*3)
                errors = [0]*len(USERS)
            
            print('Rate Limit reached , switching a user...')
            # need to implement a fallback plan (use a different user)
            switch = (switch+1) % len(USERS)
            
            
            if apis[switch] == None:            
                consumer_key, consumer_secret, access_key, access_secret, host, port =load_config(USERS[switch])
            
                auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
                auth.set_access_token(access_key, access_secret)
                #
                apis[switch] = tweepy.API(auth)
                

        else:
            print (e)
            raise
            

print ("Finished!")

#%%

reset = False
if reset:
    dbcoll.update_many({'status': 'Error'}, {'$set': {'status': 'New'}}) #find({'status': 'Error'})
#for e in errors:
#    e.update

