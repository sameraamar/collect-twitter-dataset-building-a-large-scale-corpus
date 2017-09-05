# -*- coding: utf-8 -*-
"""
Created on Thu Aug 31 10:17:20 2017

@author: my7pro
"""
import redis
import pandas as pd
from  redis_queue import RQueue 
import json

from tweepy_utils import download, init


db= redis.Redis()
q = RQueue("tweets")
    
def load_data(limit=None):    

    filename = "D:\\data\\dataset\\fsd_corpus\\fsd_corpus\\tweet_ids"
    all_id = pd.read_csv(filename, sep='	', names=["id", "name"], nrows=limit)
    
    #%%
    downloaded_file = "D:\\data\\dataset\\fsd_corpus\\fsd_corpus\\tweets_downloaded.csv"
    downloaded = pd.read_csv(downloaded_file, names=["id"], nrows=limit)
    missing = []
    
    #%%
    
    id_only = all_id['id']
    print(id_only.head(1))
    print(downloaded.head(1))
    
    
    #%%
    
    new = all_id.merge(downloaded, on=['id'],how='left')
    #temp = new[new.id.isnull()]
    #%%
    
    #print(new)
    
    missing = all_id[~all_id.id.isin(downloaded.id)]
    
    #%%
    
    print(len(missing) + len(downloaded))
    
    #%%

    counter = 0
    
    while q.pop() is not None:
        counter = 0
   
    for id, row in missing.iterrows():
        q.push(row['id'])
        #print(id, ':' , row['id'])
        
        counter += 1
 
    print('there are', counter, ' to download')
        
    
#%%

def worker_run(node, tweet_ids):
    counter = 0
    
    init()
    
    succeed = {}
    failed = {}
    
    s = 0
    f = 0
    
    #f = open(filename, 'w+')
    
    idlist = []
    for c in tweet_ids: #.iterrows():
        finished = False
        idlist.append( c ) #['id'])
    
        counter+=1    
        
        if counter % 100 == 0:
            download( idlist, succeed,failed, node)
            s += len(succeed)
            f += len(failed)
            
            #with f:
            for tw in succeed:
                tw_txt = json.dumps(succeed[tw]['json'])
                try:
                    db.hset('success', tw, tw_txt)
                except:
                    db.hset('success', tw, tw_txt)
                #json.dump(succeed[tw]['json'], f)
                #f.write('\n')
            for tw in failed:
                try:
                    db.hset('failed', tw, 'error')
                except:
                    db.hset('failed', tw, 'error')
                
                
            succeed = {}
            failed = {}
            idlist = []

            print('>>>>>>>>>>>>>>>>>>>>>  Processed: ', counter, ' success: ', s, ' failed: ', f, '(', (f/counter), '%)')
    
        
    if counter % 100 > 0:
        download( idlist, succeed,failed, 1)
        
       # with open('c:/temp/tweets.json', 'a+') as f:
       #     for tw in succeed:
       #         json.dump(succeed[tw]['json'], f)
       #         f.write('\n')    
        for tw in succeed:
            tw_txt = json.dumps(succeed[tw]['json'])
            db.hset('success', tw, tw_txt)
            #json.dump(succeed[tw]['json'], f)
            #f.write('\n')
        for tw in failed:
            db.hset('failed', tw, 'error')    
    
        print('>>>>>>>>>>>>>>>>>>>>>  Processed: ', counter, ' success: ', s, ' failed: ', f, '(', (f/counter), '%)')
            

#%%
import sys

if __name__ == "__main__":
    print (sys.argv[1:])
        
    flag = sys.argv[1] == '0'
    if flag:
        limit = int(sys.argv[2])
        if(limit <= 0):
            limit = None
            
        load_data(limit)
        print('done.')   

    else:
        counter = 0
        node = int(sys.argv[2])        
        
        finished = False
        while not finished:
            mylist = []
            for i in range(1000):
               t =  q.pop()
               if t == None:
                   finished= True
                   break
               mylist.append(t)
                
            print('i have to download: ', len(mylist))    
            worker_run(node, mylist)
            counter+=len(mylist)
            print('>>>>>>>>>>>>>>>>>>>>>  Overall Processed: ', counter)

        print('done: ' , counter)   
        input()
    

    
