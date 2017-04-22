import tweepy

import configparser
CONF_INI_FILE = 'C:/data/Personal/amdocs-laptop/data/conf.ini'


def get_tweet_id(line):
    '''
    Extracts and returns tweet ID from a line in the input.
    '''
    tw = line.strip().split()[0]
    # line = str(line).strip()

    # (tagid,_timestamp,_sandyflag) = line.split('\t')
    # (_tag, _search, tweet_id) = tagid.split(':')
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
    # for tweet in tweets:
    #    print('%s,%s' % (tweet.id, tweet.text.encode('UTF-8')))


def load_config(user='DEFAULT'):
    config = configparser.ConfigParser()
    config.read(CONF_INI_FILE)

    default = config['DEFAULT']
    host = default['mongodb_host']
    port = int(default['mongodb_port'])

    default = config[user]
    consumer_key = default['consumer_key']
    consumer_secret = default['consumer_secret']
    access_key = default['access_key']
    access_secret = default['access_secret']

    return consumer_key, consumer_secret, access_key, access_secret, host, port


if __name__ == "__main__":
    consumer_key, consumer_secret, access_key, access_secret, host, port = load_config('USER1')

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    #
    api = tweepy.API(auth)


    tweets = get_tweet_list(api, ['855791404433518594', '525752549346123777',
                         '855792892069203968', '86370333774462976'])

    file = open('c:/temp/my_tweets.txt', 'w')
    for tweet in tweets:
        file.write(str( tweet._json))

    file.close()