Collect tweets for the dataset of "Building a large scale corpus..." 
==================================

A code in python to collect tweets 
By Samer Aamar


Dataset Citation:
-----------------
McMinn, A. J., Moshfeghi, Y., & Jose, J. M. (2013, October). Building a large-scale corpus for evaluating event detection on twitter. In Proceedings of the 22nd ACM international conference on Information & Knowledge Management (pp. 409-418). ACM.?
http://dl.acm.org/citation.cfm?id=2505695

The dataset "Twitter Event Detection Dataset" can be downloaded here: http://mir.dcs.gla.ac.uk/resources/ 
The owner of the data is Andrew McMinn (email: a.mcminn.1@research.gla.ac.uk)


Dataset Structure:
------------------
All files are tab-delimited text files

all_ids.tsv - list of ~121 million tweet ids in the following format:
			<user id>	<tweet id>
		
event_descriptions.tsv - there are 506 (range from 0 to 505) topics in the following format:
			<topic_id>	"<topic title>"
			
event_categories.tsv  - a text file mapping between topics to one or more categories in the following format
			<topic_id>	<category1>,<category2>
			
			category can be: Arts, Culture & Entertainment, Disasters & Accidents, Law, etc.

relevant_tweets.tsv - list of 151k tweet ids mapped to the 505 topics in the following format
			<topic_id>	<tweet_id>
			
			one tweet can appear more than once and belong to more than one topic


Process:
----------
* load dataset as its to mongodb
* run a process of collecting the tweet details and update the mongodb 

Results (from Aug-2016):
------------------------
* Topics
	in the file event_descriptions.tsv' there are 506 (range from 0 to 505) topics
	in the file event_categories.tsv each topic  

* Relevant Tweets:
	There are 152952 rows in the relevant_tweets.tsv file
  101240 unique tweets (some of them belong to more than one topic)
		79008 - successfuly downloaded
		22232 - failed with errors (not accessible, not found, etc.)
	

* All IDs

Prerequisites:
--------------
* tweepy python package
* pymongo python package for mongodb management

Note:
-------
You are not allowed to publish dataset of twitter without a formal permission. You are also not allowed to keep the data of twitter for longer than 24h.


