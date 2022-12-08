import numpy as np 
import pandas as pd 
import tweepy as tw #For twitter data extraction
from tqdm import tqdm
import requests
import os
import json
import csv
import datetime
import dateutil.parser
import unicodedata
import time

#Adapted from Edward (2021)
#Importing token for authentication
os.environ['TOKEN'] = '' #Token removed for security purposes
def auth():
    return os.getenv('TOKEN')

#Creating headers with token to connection the the twitter API endpoint
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "cryptocurrency lang:en" #keyword in tweets
fileName = "cryptocurrencyL.csv" #name of CSV for dataset

start_time = datetime.datetime(2021, 1, 1)
delta = datetime.timedelta(days=7)
n_weeks = 54

start_list = []
end_list = []
#Time frame for 7 day intervals
while len(start_list) < 54:
  window_start = start_time if len(start_list) == 0 else window_end
  window_end = window_start + delta

  start_list.append(window_start.isoformat() + ".000Z")
  end_list.append(window_end.isoformat() + ".000Z")

max_results = 500

def create_url(keyword, start_list, end_list, max_results = max_results):
    
    search_url = "https://api.twitter.com/2/tweets/search/all" #Full historical tweet data endpoint

    #Paraters used in the query + Endpoint
    query_params = {'query': keyword,
                    'start_time': start_list,
                    'end_time': end_list,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)


# Connecting to Twitter API Endpoint
def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def append_to_csv(json_response, fileName):

    #A counter variable
    counter = 0

    #Open OR create the target CSV file
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    #Loop through each tweet
    for tweet in json_response['data']:
        
        # We will create a variable for each since some of the keys might not exist for some tweets
        # So we will account for that

        # 1. Author ID
        author_id = tweet['author_id']

        # 2. Time created
        created_at = dateutil.parser.parse(tweet['created_at'])

        # 3. Geolocation
        if ('geo' in tweet):   
            geo = tweet['geo']['place_id']
        else:
            geo = " "

        # 4. Tweet ID
        tweet_id = tweet['id']

        # 5. Language
        lang = tweet['lang']

        # 6. Tweet metrics
        retweet_count = tweet['public_metrics']['retweet_count']
        reply_count = tweet['public_metrics']['reply_count']
        like_count = tweet['public_metrics']['like_count']
        quote_count = tweet['public_metrics']['quote_count']

        # 7. source
        source = tweet['source']

        # 8. Tweet text
        text = tweet['text']
        
        # Assemble all data in a list
        res = [author_id, created_at, geo, tweet_id, lang, like_count, quote_count, reply_count, retweet_count, source, text]
        
        # Append the result to the CSV file
        csvWriter.writerow(res)
        counter += 1

    # When done, close the CSV file
    csvFile.close()

    # Print the number of tweets for this iteration
    print("# of Tweets added from this response: ", counter)

#Total number of tweets collected
total_tweets = 0

# Creating CSV
csvFile = open(fileName, "a", newline="", encoding='utf-8')
csvWriter = csv.writer(csvFile)

#Headers for CSV
csvWriter.writerow(['author id', 'created_at', 'geo', 'id','lang', 'like_count', 'quote_count', 'reply_count','retweet_count','source','tweet'])
csvFile.close()

for i in range(0, len(start_list)):

    
    count = 0
    max_count = 500 # Max tweets per time period
    flag = True
    next_token = None
    
    # Check if flag is true
    while flag:
        # Check if max_count reached
        if count >= max_count:
            break
        print("-------------------")
        print("Token: ", next_token)
        url = create_url(keyword, start_list[i], end_list[i], max_results)
        json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
        result_count = json_response['meta']['result_count']

        if 'next_token' in json_response['meta']:
            # Next token gets passed to other calls
            next_token = json_response['meta']['next_token']
            print("Next Token: ", next_token)
            if result_count is not None and result_count > 0 and next_token is not None:
                print("Start Date: ", start_list[i])
                append_to_csv(json_response, fileName)
                count += result_count
                total_tweets += result_count
                print("Total # of Tweets added: ", total_tweets)
                print("-------------------")
                time.sleep(5)                
        # If no next token exists
        else:
            if result_count is not None and result_count > 0:
                print("-------------------")
                print("Start Date: ", start_list[i])
                append_to_csv(json_response, fileName)
                count += result_count
                total_tweets += result_count
                print("Total # of Tweets added: ", total_tweets)
                print("-------------------")
                time.sleep(5)
            
        
            flag = False
            next_token = None
        time.sleep(5)

print("Total number of results: ", total_tweets)