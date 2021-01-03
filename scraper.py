import tweepy
import psycopg2

from sqlalchemy import create_engine
from sqlalchemy import insert
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert

from textblob import TextBlob
from nltk.corpus import stopwords
import re


consumer_key = 'your consumer_key'
consumer_secret = 'your consumer_secret_key'
access_token = 'your access token'
access_secret = 'your access secret key'
tweetsPerQry = 100
maxTweets = 100000
hashtag = "your hashtag"

#clean tweet text

def clean_text(text):
  ex_list = ['rt', 'http', 'RT']
  exc = '|'.join(ex_list)
  text = re.sub(exc, ' ' , text)
  text = text.lower()
  words = text.split()
  stopword_list = stopwords.words('english')
  words = [word for word in words if not word in stopword_list]
  clean_text = ' '.join(words)
  return clean_text

def sentiment_score(text):
  analysis = TextBlob(text)
  if analysis.sentiment.polarity > 0:
    return 1
  elif analysis.sentiment.polarity == 0:
    return 0
  else:
    return -1


authentication = tweepy.OAuthHandler(consumer_key, consumer_secret)
authentication.set_access_token(access_token, access_secret)
api = tweepy.API(authentication, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
maxId = -1
tweetCount = 0

meta_list = []

while tweetCount < maxTweets:
  if(maxId <= 0):
    newTweets = api.search(q=hashtag, count=tweetsPerQry, result_type="recent", tweet_mode="extended")
  else:
    newTweets = api.search(q=hashtag, count=tweetsPerQry, max_id=str(maxId - 1), result_type="recent", tweet_mode="extended")

  if not newTweets:
    print("Done")
    break

  for tweet in newTweets:
    username = tweet.user.name
    created_at = str(tweet.created_at)
    tweet_text = tweet.full_text
    tweet_text_sent = tweet.full_text
    retweet_count = tweet.retweet_count
    fav_count = tweet.favorite_count
    media_source = tweet.source
    tweet_text_sent = clean_text(tweet_text_sent)
    result_score = sentiment_score(tweet_text_sent)
    data_dict = {
    "username":username,
    "created_at":created_at,
    "tweet_text":tweet_text,
    "retweet_count":retweet_count,
    "fav_count":fav_count,
    "media_source":media_source,
    "semtiment_score":result_score
    }
    meta_list.append(data_dict)
  tweetCount += len(newTweets)
  maxId = newTweets[-1].id

print(meta_list)

#connect engine
engine = create_engine('postgresql://username:yourpassword@servername:portnumber/database')

conn = engine.connect()
metadata = MetaData()
print(metadata.tables)
# reflect db schema to MetaData
trans = conn.begin()
metadata.reflect(bind=engine)

table_name = metadata.tables['table_name']

insert_stmt = insert(table_name)
#if you have constraint in your database, and you just want to update unique record
#do_nothing_stmt = insert_stmt.on_conflict_do_nothing(constraint='')
result_proxy = conn.execute(insert_stmt, meta_list)
trans.commit()
conn.close()
