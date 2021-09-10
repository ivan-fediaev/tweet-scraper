from nltk import sentiment
import tweepy
import time
from pandas import DataFrame
import threading
import logging
import json
import html
import re
import sqlite3
from nltk.sentiment import SentimentIntensityAnalyzer

def scrape_tweets(handle):
    
    with open("API_keys.json") as json_file:
        API_keys = json.load(json_file)
        json_file.close()

    consumer_key        = API_keys["consumer_key"]
    consumer_secret     = API_keys["consumer_secret"]

    access_token        = API_keys["access_token"]
    access_token_secret = API_keys["access_token_secret"]

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    logging.info("Fetching the inital tweet from @{}".format(handle))
    try:
        latest_tweet = api.user_timeline(screen_name=handle, count=1, include_rts=False, tweet_mode="extended")[0]
    except Exception as e:
        logging.exception("An exception has occured: ", e)

    scraped_tweets = []
    scraped_tweets.append(latest_tweet)
    latest_tweet_id = latest_tweet.id
    retries = 10
    times_to_fetch = 500

    logging.info("Starting the fetching loop for @{}".format(handle))
    while times_to_fetch > 0:
        logging.info("Fetching up to 20 tweets from @{}".format(handle))
        tweets = api.user_timeline(screen_name=handle, count=20, include_rts=False, max_id=latest_tweet_id-1, tweet_mode="extended")
        
        if len(tweets) == 0:
            retries -= 1
            logging.info("No tweets fetched from the handle {}, trying again. {} retries left.".format(handle,retries))
            
            if retries == 0:
                logging.info("0 retries left in fetching tweets from @{}, breaking from the fetching loop.".format(handle))
                break
            
            logging.info("Sleeping for 30 seconds for @{} before trying to fetch tweets again.".format(handle))
            time.sleep(30)
            continue
        
        retries = 10
        latest_tweet_id = tweets[-1].id
        scraped_tweets.extend(tweets)
        logging.info("{} tweets scraped from @{} so far.".format(len(scraped_tweets),handle))
        
        times_to_fetch -= 1
        if times_to_fetch == 0:
            logging.info("Done fetching tweets for @{}".format(handle))
            break
        
        logging.info("Sleeping for 30 seconds for @{} before fetching tweets again.".format(handle))
        time.sleep(30)

        logging.info("{} times left to fetch.".format(times_to_fetch))

    logging.info("Converting scraped tweets from @{} into a data frame.".format(handle))
    out_tweets = []
    sia = SentimentIntensityAnalyzer()

    con = sqlite3.connect("TweetScraper")
    cur = con.cursor()

    for tweet in scraped_tweets:
        tweet_text = html.unescape(tweet.full_text)
        tweet_text = re.sub("http://\S+|https://\S+|@[\w]+|", '', tweet_text)
        tweet_text = re.sub("\n", ' ', tweet_text)
        stats = sia.polarity_scores(tweet_text)

        tweet_row = [tweet.id_str, tweet.created_at, tweet.favorite_count, tweet.retweet_count, tweet_text, stats["neg"], stats["neu"], stats["pos"], stats["compound"]]
        out_tweets.append(tweet_row)

        sql_command = '''INSERT INTO {}(id, created_at, favorite_count, retweet_count, text, neg, neu, pos, compound) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);'''.format(handle)
        tweet_tuple = (tweet.id_str, tweet.created_at, tweet.favorite_count, tweet.retweet_count, tweet_text, stats["neg"], stats["neu"], stats["pos"], stats["compound"])
        cur.execute(sql_command, tweet_tuple)

    df = DataFrame(out_tweets,columns=["id","created_at","favorite_count","retweet_count", "text", "neg", "neu", "pos", "compound"])

    logging.info("Writing data frame to {}_tweets.csv".format(handle))

    df.to_csv("{}_tweets.csv".format(handle),index=False)


    con.commit()
    con.close()


def main():
    elon_tweets = threading.Thread(target=scrape_tweets,args=("elonmusk",))
    cnn_tweets  = threading.Thread(target=scrape_tweets,args=("cnnbrk",))

    logging.info("Starting the threads.")
    elon_tweets.start()
    cnn_tweets.start()

    logging.info("Joining the threads.")
    elon_tweets.join()
    cnn_tweets.join()

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filename="scraping.log", level=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.info("Started the script.")
    main()
    logging.info("Finished the script.")