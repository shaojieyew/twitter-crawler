import requests
import bs4
import time
from bs4 import BeautifulSoup
from requests_oauthlib import OAuth1
from datetime import datetime, date, timedelta
import pytz
import os, sys, os.path
import json
class TweetCrawler:
    def writing(self, key, val):
        self.last_crawled[key]=val
        target = open('./last_crawled.txt', 'w+')
        target.write(str(self.last_crawled))

    def reading(self):
        if(os.path.isfile("./last_crawled.txt")==False):
            self.last_crawled= {}
        else:
            self.last_crawled = eval(open('./last_crawled.txt', 'r').read())

    auth_params = {
            'app_key':'Ms7gmRrmtpHZdCLGGzMhWkfdm',
            'app_secret':'Ofv4FE9vB8HJaw2CyZbAgwFGndtD8xfYapPCXhuqsP7KCHWV4s',
            'oauth_token':'1156816304549928961-lQ6345u4c4K6UYQhQvuZWPTAFLactF',
            'oauth_token_secret':'vUQBbYnosQcP4RqMUMvPgJjJl8vWPly7hd9YvMMoCChQK'
        }
    def __init__(self,keyword,days_ago,output):
        # Creating an OAuth Client connection
        self.reading()
        self.output = output
        self.auth = OAuth1 (
            TweetCrawler.auth_params['app_key'],
            TweetCrawler.auth_params['app_secret'],
            TweetCrawler.auth_params['oauth_token'],
            TweetCrawler.auth_params['oauth_token_secret']
        )
        self.days_ago=days_ago
        self.keyword=keyword
    def crawl(self,key1,val1):
        # url according to twitter API
        url_rest = "https://api.twitter.com/1.1/search/tweets.json"
        params = {
            'q': self.keyword+' -filter:retweets -filter:replies', 
            'count': 10000, 
            'lang': 'en',  
            'result_type': 'recent',  
            'tweet_mode': 'extended', 
            'text':'full_text',
            #'max_id':'1173460878227845120',
            #'since_id':'1173589160722075648'
        }
        if(val1 is not None):
            params[key1]=val1
        results = requests.get(url_rest, params=params, auth=self.auth)
        tweets = results.json()
        #print(tweets['statuses'][len(tweets['statuses'])-1])
        #messages = [BeautifulSoup(tweet['text'], 'html5lib').get_text() for tweet in tweets['statuses']]
        
        return tweets['statuses']
    def start(self):
        N_DAYS=self.days_ago
        reach_oldest_tweet = 0
        max_id=None
        min_id=None
        next_max_id=None
        next_min_id=None
        result=None
        while(1):
            
            if(reach_oldest_tweet==1):
                param1 = "since_id"
                val1 = next_min_id
            else:
                param1 = "max_id"
                val1 = next_max_id
            print(param1+":"+str(val1))
            tweets = self.crawl(param1,val1)
            if(reach_oldest_tweet==1 and len(tweets)<=1):
                self.writing(self.keyword,next_min_id)
                #print("waiting for new tweets... "+str(param1)+":"+str(val1))
                time.sleep(60*5)
                continue
            if(reach_oldest_tweet==0 and len(tweets)<=1):
                reach_oldest_tweet=1
            else:
                oldest_tweet= tweets[len(tweets)-1]
                recent_tweet= tweets[0]
                tweets_min_id = BeautifulSoup(oldest_tweet['id_str'], 'html5lib').get_text()
                tweets_min_time = BeautifulSoup(oldest_tweet['created_at'], 'html5lib').get_text()
                tweets_min_time = datetime.strptime(tweets_min_time, '%a %b %d  %H:%M:%S %z %Y')
                tweets_max_id = BeautifulSoup(recent_tweet['id_str'], 'html5lib').get_text()
                tweets_max_time = BeautifulSoup(recent_tweet['created_at'], 'html5lib').get_text()
                tweets_max_time = datetime.strptime(tweets_max_time, '%a %b %d  %H:%M:%S %z %Y')
                if(min_id is None or reach_oldest_tweet==0):
                    min_id=tweets_min_id
                if(max_id is None or reach_oldest_tweet==1):
                    max_id=tweets_max_id
                utc=pytz.UTC
                today = utc.localize(datetime.today())
                n_days_ago = today - timedelta(days=N_DAYS)
            if(tweets_min_time<=n_days_ago):
                reach_oldest_tweet=1
            if(self.keyword in self.last_crawled.keys()):
                print(str(tweets_min_id)+" - "+str(self.last_crawled[self.keyword]))
            if(self.keyword in self.last_crawled.keys() and int(tweets_min_id)<=self.last_crawled[self.keyword]):
                reach_oldest_tweet=1
            if(reach_oldest_tweet==1):
                next_min_id = int(max_id)+1
            if(reach_oldest_tweet==0):
                next_max_id=int(min_id)-1

            print("count: "+str(len(tweets))+ "  id:"+str(tweets_min_id)+"-"+str(tweets_max_id))
            print("tweet duration: "+str(tweets_min_time)+" - "+str(tweets_max_time))
            if(result is None):
                result = tweets
            else:
                result= result+tweets
            if(self.output==1):
                for tweet in tweets:
                    id = BeautifulSoup(tweet['id_str'], 'html5lib').get_text()
                    if(os.path.exists("./crawled_data")==False):
                        os.mkdir('./crawled_data')

                    with open("./crawled_data/"+id+".json", 'w') as json_file:
                        json.dump(tweet, json_file)
                    #message = BeautifulSoup(tweet['full_text'], 'html5lib').get_text()
                    #id = BeautifulSoup(tweet['id_str'], 'html5lib').get_text()
                    #time = BeautifulSoup(tweet['created_at'], 'html5lib').get_text()
                    #print('--------'+time+'---------')
                    #print(id)
                    #print(message)
                    #print('-----------------------------------------------')

            time.sleep(3)
def main():
    crawler = TweetCrawler('AAPL',2,1)
    crawler.start()
main()
print("Exited")
