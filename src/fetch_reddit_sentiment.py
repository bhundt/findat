import os
import requests
import time
import datetime
from datetime import date, timedelta
import pandas as pd
import numpy as np
import sys

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import spacy

# Update lexicon for better accuracy
positive_words = 'buy bull long support undervalued underpriced cheap upward rising trend moon rocket hold hodl breakout call beat support buying holding high profit stonks yolo'
negative_words = 'sell bear bubble bearish short overvalued overbought overpriced expensive downward falling sold sell low put miss resistance squeeze cover seller loss '
pos = {i: 5 for i in positive_words.split(" ")}
neg = {i: -5 for i in negative_words.split(" ")}
stock_lexicons = {**pos, **neg}
analyser = SentimentIntensityAnalyzer()
analyser.lexicon.update(stock_lexicons)

# named entity reconition
nlp = spacy.load("en_core_web_sm")
blacklist_orgs = ["WSB", "Robinhood", "SEC", "Fed", "CNBC", "Citadel", "RH", "FDA", "Fidelity", "Reddit"]

def get_comments_from_entry(entry_id):
    url = 'https://api.pushshift.io/reddit/comment/search/?link_id={}&limit=2000&sort_type=score&sort=desc'.format(str(entry_id))
    r = requests.get(url)
    comments = [x['body'] for x in r.json()['data']]
    return comments

def filter_comments_for_org(comments):
    comments_with_org = []
    for comment in comments:
        doc = nlp(comment)
        for ent in doc.ents:
            if (ent.label_ == "ORG") and (ent.text.lower() not in [x.lower() for x in blacklist_orgs]):
                comments_with_org.append(comment)
    return comments_with_org

def calc_sentiment_for_comments(comments):
    result_dict = {'pos': 0.0, 'neu': 0.0, 'neg': 0.0, 'compound':0.0}
    for comment in comments:
        sentiment_dict = analyser.polarity_scores(comment)
        result_dict['pos'] = result_dict['pos'] + sentiment_dict['pos']
        result_dict['neu'] = result_dict['neu'] + sentiment_dict['neu']
        result_dict['neg'] = result_dict['neg'] + sentiment_dict['neg']
        result_dict['compound'] = result_dict['compound'] + sentiment_dict['compound']
    
    result_dict['pos'] = result_dict['pos'] /len(comments)
    result_dict['neu'] = result_dict['neu'] /len(comments)
    result_dict['neg'] = result_dict['neg'] /len(comments)
    result_dict['compound'] = result_dict['compound'] /len(comments)
    return result_dict

def get_sentiments_for_reddit_comments(date):
    start_timestamp = int(time.mktime(date.timetuple()))
    end_timestamp = int(time.mktime((date + datetime.timedelta(days=1)).timetuple()))

    # get daily discussion
    #url = 'https://api.pushshift.io/reddit/search/submission/?subreddit=wallstreetbets&size=1&after={}&before={}&title=Daily%20Discussion'.format(start_timestamp, end_timestamp)
    url = 'https://api.pushshift.io/reddit/search/submission/?subreddit=wallstreetbets&size=10&title={}'.format(date.strftime("%B %d, %Y"))
    r = requests.get(url)
    
    overall_comment = []
    for entry in r.json()['data']:
        entry_id = entry['id']

        # get comments for discussion entry
        comments = get_comments_from_entry(entry_id)
        comments = filter_comments_for_org(comments)
        
        overall_comment.append(comments)

    # calc sentiment per comment containing an organization
    result_dict = calc_sentiment_for_comments(overall_comment)
    return result_dict

def prep_dataframe(date, sentiment_dict):
    return pd.DataFrame( { 
        'Date': np.datetime64(date),
        'POSITIVE': sentiment_dict['pos'],
        'NEUTRAL': sentiment_dict['neu'],
        'NEGATIVE': sentiment_dict['neg'],
        'COMPOUND': sentiment_dict['compound'],
    }, index=[1])

def add_empty_row_for_date(date, df):
    return df.append({'Date': np.datetime64(date),
                                'POSITIVE': np.nan,
                                'NEUTRAL': np.nan,
                                'NEGATIVE': np.nan,
                                'COMPOUND': np.nan,
                                 }, ignore_index=True)


def make_empty_result_df():
    return pd.DataFrame( {
                        'Date': np.datetime64(),
                        'POSITIVE':float(), 
                        'NEUTRAL': float( ),
                        'NEGATIVE': float( ),
                        'COMPOUND': float()
                    }, index=[])

def store_in_database(data, filename):
    # create empty file with correct headers if necessary
    if os.path.exists(filename) is not True:
        make_empty_result_df().to_csv(filename, sep=';', decimal='.', encoding='utf-8', index=False)
    
    # add new entry
    df = pd.read_csv(filename, sep=';', decimal='.', encoding='utf-8', parse_dates=['Date'])
    df = df.append(data).reset_index(drop=True)
    
    # check/remove duplicates in date column
    df = df.drop_duplicates(subset=['Date'], keep='last')

    # save to file
    df.to_csv(filename, sep=';', decimal='.', encoding='utf-8', index=False)


def main():
    start_date = datetime.date(2020, 1, 1)
    end_date = date.today() - timedelta(days=1)
    delta = timedelta(days=1)

    running_date = start_date
    while running_date <= end_date:
        try:
            sentiment_dict = get_sentiments_for_reddit_comments(running_date)
            sentiment_df = prep_dataframe(running_date, sentiment_dict)
            print("Overall sentiment on {} is: {}".format(running_date.strftime("%Y-%m-%d"), sentiment_dict))
        except:
            print('Error: ', sys.exc_info())
            sentiment_df = make_empty_result_df()
            sentiment_df = add_empty_row_for_date(running_date, sentiment_df)
            print('ERROR: failed to get sentiment for {}'.format(running_date.strftime("%Y-%m-%d")))
        
        store_in_database(sentiment_df, 'data/database_reddit_sentiment.csv')

        running_date += delta

if __name__ == "__main__":
    # we assume this code is in /src while data is in /data. Since we do not want to assume a cwd we switch to src and than move one up
    path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(path)
    os.chdir(os.path.pardir)

    main()
