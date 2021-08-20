import os
import sys
import time
import datetime
import re
from datetime import date, timedelta

import pandas as pd
import numpy as np
import spacy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# hacky hack to get relative import
sys.path.append( os.path.dirname( os.path.dirname(os.path.realpath(__file__)) ) )
from helper.timespan_helper import get_timestamp_from_date

# Update lexicon for better accuracy of sentiment analysis
positive_words = 'buy bull long support undervalued underpriced cheap upward rising trend moon rocket hold hodl breakout call beat support buying holding high profit stonks yolo'
negative_words = 'sell bear bubble bearish short overvalued overbought overpriced expensive downward falling sold sell low put miss resistance squeeze cover seller loss crash rip'
pos = {i: 5 for i in positive_words.split(" ")}
neg = {i: -5 for i in negative_words.split(" ")}
stock_lexicons = {**pos, **neg}
analyser = SentimentIntensityAnalyzer()
analyser.lexicon.update(stock_lexicons)

# named entity reconition
nlp = spacy.load("en_core_web_sm") # spacy.load("en_core_web_trf")
blacklist_orgs = [x.lower() for x in ["WSB", "Robinhood", "SEC", "Fed", "CNBC", "Citadel", "RH", "FDA", "Fidelity", "Reddit", 'wallstreetbets']]

def get_sentiment(input_str):
    try:
        assert(len(input_str) > 0)
        sentiment = analyser.polarity_scores(str(input_str))
    except (AttributeError, TypeError):
        raise AssertionError('Input variable either empty or not a string')

    return sentiment

def check_for_org(input_str, blacklist=[]):
    doc = nlp( remove_emoji_from(input_str) )
    org_list = [entity for entity in doc.ents if entity.label_ == "ORG"]
    blacklist_check = list(set(org_list) & set(blacklist))
    return True if org_list and not blacklist_check else False

def remove_emoji_from(text):
    regrex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags = re.UNICODE)
    return regrex_pattern.sub(r'',text)

if __name__ == "__main__":
    pass
    # text = 'Apple stocks are really, really awesome! 🚀'
    # print(check_for_org(text))
    # print(get_sentiment(text))