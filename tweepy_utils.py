# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 11:32:02 2016

@author: SAMERA
"""

#%%

#%%

import configparser
import time


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
    
    print('Loading conf for section: ' , user)
    
    default = config['DEFAULT']
    host = default['mongodb_host']
    port = int ( default['mongodb_port']  )
    
    if user == 'DEFAULT':
        return
        
    default = config[user]
    consumer_key = default['consumer_key']
    consumer_secret = default['consumer_secret']
    access_key = default['access_key']
    access_secret = default['access_secret']

    
    return consumer_key, consumer_secret, access_key, access_secret, host, port

#%%
times =  {}
CONF_INI_FILE = ''

def init(configFile = 'd:/data/conf.ini'):
    global CONF_INI_FILE
    CONF_INI_FILE = configFile
    
    load_config()
    global times
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
        if dup != len(werrors):
            raise

def get_tweets_bulk(twapi, idlist, posts, errors):
    if len(idlist)==0:
        return 

    global times
        
    starttime = time.time()
    tweets = get_tweet_list(twapi, idlist)

    times['twitter'] += time.time() - starttime
    times['count2'] += 1
    
    #print('download info for ', len(idlist), ' tweets')
    
    #for t in idlist:
    #    posts[t] = t
        
    #if True:
    #    return len(posts)
    
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
        #bulk2.find({'_id': tid}).replace_one(temp)
        #bulk2.find({'_id': tid}).update({'$set': {'json': j, 'status': 'Loaded'}})
        posts[tid] = temp
        #bulk.find({'_id': tid}).update({'$set': {'json': j, 'status': 'Loaded'}})
        
        #idlist.remove( tid )
    
    for u in newstatus:
        if newstatus[u] == 'Error':
            errors[u] = {'_id':u, 'user_id':'Error'}
            #idlist.remove( u )


    times['mongodb'] += time.time() - starttime
    times['count1'] += 1

    return len(tweets)
#%%
    
from tweepy import TweepError, RateLimitError

def download(idlist, succeed, failed, node):
    global times
    USERS=[ 'USER1', 'USER2', 'USER3', 'USER4', 'USER5', 'USER6', 'USER7' , 'USER8' , 'USER9' ]
    
    if node < 0:
        node = 0
        
    switch = node % len(USERS)
  
    errors = [x*2 for x in range(len(USERS))]
    apis = [None]*len(USERS)
    
    consumer_key, consumer_secret, access_key, access_secret, host, port =load_config(USERS[switch])
    
    if apis[switch] == None:
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_key, access_secret)
        #
        #apis[switch] = tweepy.API(auth)
        apis[switch] = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        print('Opened connection to API')
        
    #%%
    
    
    itr = 0

    while True:
         
        startgloabl = starttime = time.time()
    
        itr += 1
        
        idlist_tmp = []
        for t in idlist:
            #if t is None:
            #    continue
            
            t = int(t)
            if not (t in succeed or t in failed):
                idlist_tmp.append(t)
                
            
        finished = len(idlist_tmp)==0
        #print('iteration' , itr, ' -: ', len(idlist_tmp))
    
        times['mongodb'] += time.time() - starttime
        times['count1'] += 1
        if finished:
            break
        if len(idlist) == 0:
            continue
        
        try:
            bulk_res = get_tweets_bulk(apis[switch], idlist_tmp, succeed, failed)
    
            print(USERS[switch],',',min(idlist_tmp),'..',max(idlist_tmp),'. ')
            if itr%10==0:
                cLoaded = len(succeed)
                cErrors = len(failed)      
                p = 1000.0 * cErrors/(cErrors+cLoaded)
                p = int(p) / 10.0
                print('\tsuccess :', cLoaded, ' Error: ', cErrors, ' (' , p, '%)', end='')
    
                    
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
                    apis[switch] = tweepy.API(auth)
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
                    
    
    print ("Finished!", '\tsuccess :', len(succeed), ' Error: ', len(failed))
    