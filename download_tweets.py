# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 11:32:02 2016

@author: SAMERA
"""

#%%

#%%

import configparser

CONF_INI_FILE = 'c:/data/conf.ini'

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
import time

times =  {
        'mongodb' : 0.0, 
        'count1' : 0,
        'twitter' : 0.0,
        'count2' : 0
    }


#%%

import tweepy
from pymongo.errors import BulkWriteError

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

def commit_bulk(bulk):
    try:
        bulk.execute()
    except BulkWriteError as bwe:
        #print(bwe.details)
        werrors = bwe.details['writeErrors']
        dup = 0
        for rrr in werrors: 
            if rrr['code']==11000:
                dup+=1
            print(rrr)
        if len(dup) != len(werrors):
            raise

def get_tweets_bulk(twapi, idlist, dball_ids, dbposts, dberrors):
    
    if len(idlist)==0:
        return 

    global times
        
    starttime = time.time()
    tweets = get_tweet_list(twapi, idlist)

    bulk1 = dball_ids.initialize_unordered_bulk_op()
    bulk2 = dbposts.initialize_unordered_bulk_op()
    bulk3 = dberrors.initialize_unordered_bulk_op()

    times['twitter'] += time.time() - starttime
    times['count2'] += 1
    
    
    newstatus = {}
       
    starttime = time.time()
    for t in idlist:
        t = int(t)
        newstatus[t] = 'Error'
        #bulk.find({'_id': t}).update({'$set': {'status': 'Error'}})
        
    for t in tweets:
        #j = json.dumps(t._json)
        j = t._json
        tid = j['id']
        newstatus[tid] = 'Loaded'
        temp = {'_id': tid, 'json': j}
        bulk2.find({'_id': tid}).replace_one(temp)
        #bulk2.insert(temp)
        #bulk.find({'_id': tid}).update({'$set': {'json': j, 'status': 'Loaded'}})
    
    for u in newstatus:
        if newstatus[u] == 'Error':
            bulk3.insert({'_id':u, 'user_id':'Error'})
        #bulk1.find({'_id': u}).update( {'$set': {'status':  newstatus[u]}} )
        bulk1.find({'_id': u}).remove()
    
    commit_bulk(bulk2)
    commit_bulk(bulk3)
    commit_bulk(bulk1)

    times['mongodb'] += time.time() - starttime
    times['count1'] += 1

    return len(tweets)
#%%
import sys

USERS=[ 'USER1', 'USER2', 'USER3', 'USER4', 'USER5', 'USER6', 'USER7' ]
switch=0


#*****
node = 0
if len(sys.argv) > 1:
    node = int(sys.argv[2])
    if node < 0:
        node = 0
        
    switch = node % len(USERS)
        
skip = 1000 * node
print("In the query i wil skip ", skip)

#*****

errors = [x*2 for x in range(len(USERS))]
apis = [None]*len(USERS)


consumer_key, consumer_secret, access_key, access_secret, host, port =load_config(USERS[switch])

if apis[switch] == None:
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    #
    #apis[switch] = tweepy.API(auth)
    apis[switch] = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

#%%
from pymongo import MongoClient
import time, pymongo
from tweepy import TweepError, RateLimitError

client = MongoClient(host, int(port))
#client = MongoClient("mongodb://"+host+":"+port)

db = client['events2012-a']
coll_posts = db.posts
coll_ids = db.ids
coll_errors = db.errors

itr = 0
while True:
    times =  {
        'mongodb' : 0.0, 
        'count1' : 0,
        'twitter' : 0.0,
        'count2' : 0
    }    
    
    startgloabl = starttime = time.time()
    cursor = coll_ids.find({}).sort([('_id', pymongo.ASCENDING)]).skip(skip).limit(100)
    
    itr += 1
    idlist = []
    for c in cursor:
        idlist.append( c['_id'])
        
    times['mongodb'] += time.time() - starttime
    times['count1'] += 1
    if len(idlist) == 0:
        break
    
    try:
        bulk_res = get_tweets_bulk(apis[switch], idlist, coll_ids, coll_posts, coll_errors)

        print(USERS[switch],',',min(idlist),'..',max(idlist),'. ')
        if itr%10==0:
            cLoaded = coll_posts.find({}).count()
            cErrors = coll_errors.find({}).count()      
            print('\tleft: ', coll_ids.find({}).count(), 'success :', cLoaded, ' Error: ', cErrors, end='')

                
        print('\tmongodb avg: ', "%.2f" % (times['mongodb'] ), end=', ')
        print('twitter avg: ', "%.2f" % (times['twitter']  ), end=', ')
        print('global time: ', "%.2f" % ((time.time()-startgloabl) ))

    except (RateLimitError , TweepError ) as e:
        
        if (type(e) == TweepError and str(e)[-3:] == '429') or isinstance(e, RateLimitError):
            errors[switch] = time.time()
            
            delta = [errors[i]-errors[i-1] for i  in range(1,len(errors))]

            if max(delta) < 2:
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
                apis[switch] = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
                

        else:
            print (e)
            print ("Tweepy error! go to sleep 5 minutes")
            time.sleep(5*60)
        
        pass

    except Exception as e:
        print (e)
        print ("Some exception happened! go to sleep 5 minutes")
        time.sleep(5*60)
                

print ("Finished!")

#%%

reset = False
if reset:
    coll_ids.update_many({'status': 'Error'}, {'$set': {'status': 'New'}}) #find({'status': 'Error'})
    #coll_.update_many({'status': 'Error'}, {'$set': {'status': 'New'}}) #find({'status': 'Error'})
#for e in errors:
#    e.update

