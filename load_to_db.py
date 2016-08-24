# -*- coding, "utf-8 -*-
"""
Created on Sun Aug 14 08:57:28 2016

@author, "SAMERA
"""

#%%
#connect to mongodb

from datetime import datetime
from pymongo import MongoClient

client = MongoClient()
client = MongoClient("mongodb://localhost:27017")

db = client.events2012

#%%%%
#load topics:
import re, pymongo

folder = 'C:/Users/samera/Downloads/Events2012/'
all_ids_file = 'all_ids.tsv'
topics_file = 'event_descriptions.tsv'
categorized_file = 'relevant_tweets.tsv'

topics = {}

file = open(folder + topics_file, 'r')
for line in file:
    if line.strip() == '':
        continue
    match = re.search(r'(\d+)\t[\"]*([^\n]+)[\"]*[\n]*$', line)
    
    if match:
        topic_id = int(match.group(1))
        topic_title = match.group(2)
        topics[topic_id] =  {'_id' : topic_id, 'topic': topic_title}


file.close()

try:
    db.topics.insert_one({'_id': -1})
    info = db.topics.index_information()
    if not '_id_1' in info:
        db.topics.create_index([("_id", pymongo.ASCENDING)], unique=True, background=True)
    
    db.topics.delete_many({})
    

    db.topics.insert_many(topics.values())        
except:
    print (topic_id,  topic_title)

#%%

# load tweet ids
import pymongo
from pymongo.errors import BulkWriteError

file = open(folder + categorized_file, 'r')
categorized ={}
for line in file:
    line = line.strip()
    if line == '':
        continue 
    
    v = line.split()

    tweet_id = int(v[1])
    topic_id = int(v[0])
    cat = {}
    cat = categorized.get(tweet_id, {'_id': tweet_id, 'status': 'New', 'topics' : [{'id' : topic_id}]})
    if not {'id' : topic_id} in cat['topics']:
        cat['topics'].append( {'id' : topic_id} )
    categorized[tweet_id] = cat
    

    #db.categorized.find({"topics.id" : 40})

file.close()


#relevance_judgments
dbcoll = db.categorized

try:
    dbcoll.delete_one({'_id' : -1})
    dbcoll.insert_one({'_id' : -1})
    dbcoll.delete_many({})
    
    info = dbcoll.index_information()
    if not '_id_1' in info:
        dbcoll.create_index([("_id", pymongo.ASCENDING)], unique=True, background=True)
    
    dbcoll.insert_many(categorized.values())        
except Exception as e:
    print (e)



#%%

dbcoll = db.posts
dbcoll.delete_one({'_id' : -1})
dbcoll.insert_one({'_id' : -1})

info = dbcoll.index_information()
if not '_id_1' in info:
    dbcoll.create_index([("_id", pymongo.ASCENDING)], unique=True, background=True)
if not 'status_1' in info:
    dbcoll.create_index([("status", pymongo.ASCENDING)], background=True)

    
count = 0
#bulk = dbcoll.initialize_ordered_bulk_op()
bulk = dbcoll.initialize_unordered_bulk_op()
out = open('errors.txt', 'w')
file = open(folder + all_ids_file , 'r')
cerrors = 0
cwrite = 0
ctotal = 0
for line in file:
    line = line.strip()
    if line == '':
        continue 
    
    v = line.split()
    tweet = {}
    tweet['_id'] = int(v[1])
    tweet['user_id'] = v[0]
    
    if tweet['_id'] in categorized:
        tweet['topics'] = categorized[tweet['_id']]
    
    tweet['status'] = 'New'
    
    #tweet['topics'] = categorized[tweet['_id']]
    
    bulk.insert(tweet)
    #dbcoll.insert_one(tweet)
    
    count+=1
    if count>50000:
        #dbcoll.insert_many(ids)
        werrors = {}
        try:
            result = bulk.execute()
            #all is good
        except BulkWriteError as bwe:
            #print(bwe.details)
            werrors = bwe.details['writeErrors']
            pass
        
        #handle errors
        cerrors += len(werrors)
        for e in werrors:
            out.write( str(e['op']['_id']) )
            out.write('\t' + e['errmsg'] + '\n' )       
        ctotal += count
        cwrite = dbcoll.count()
        
        print('Total: ', ctotal, ': written ', cwrite, ', errors ', cerrors, '(%', 100.0*cerrors/ctotal,')')        
        
        bulk = dbcoll.initialize_unordered_bulk_op()
        count=0


if count>0:
    werrors = {}
    try:
        result = bulk.execute()
        #all is good
    except BulkWriteError as bwe:
        #print(bwe.details)
        werrors = bwe.details['writeErrors']
        pass
    
    #handle errors
    cerrors += len(werrors)
    for e in werrors:
        out.write( str(e['op']['_id']) )
        out.write('\t' + e['errmsg'] + '\n' )       
    ctotal += count
    cwrite = dbcoll.count()
    
    print('Total: ', ctotal, ': written ', cwrite, ', errors ', cerrors, '(%', 100.0*cerrors/ctotal,')')        
    
out.close()
file.close()


#cursor = db.find({})
#count = 0
#for doc in cursor:
#    fname = doc['tail']
#    status = doc.get('status', 'New')
#    print ('Rows, "', client.tweetsdb[fname].count(), '. File, "', fname)
#    count+=client.tweetsdb[fname].count()
#print('Total, "', count)


