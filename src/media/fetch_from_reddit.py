import json
import os
from posixpath import join
import sys
import time
import datetime
from datetime import date, timedelta
from pandas.core.indexes.base import maybe_extract_name
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import numpy as np

# from: https://stackoverflow.com/questions/667508/whats-a-good-rate-limiting-algorithm
def RateLimited(maxPerSecond):
    minInterval = 1.0 / float(maxPerSecond)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args,**kargs):
            elapsed = time.time() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait>0:
                time.sleep(leftToWait)
            ret = func(*args,**kargs)
            lastTimeCalled[0] = time.time()
            return ret
        return rateLimitedFunction
    return decorate

def get_comments_from_entry(entry_id):
    url = 'https://api.pushshift.io/reddit/comment/search/?link_id={}&limit=2000&sort_type=score&sort=desc'.format(str(entry_id))
    
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status=5, status_forcelist=[502, 503, 504, 429])
    s.mount('http://', HTTPAdapter(max_retries=retries))
    r = s.get(url)
    
    comments = [x['body'] for x in r.json()['data']]
    return comments

def get_expected_number_of_entries(start_date, end_date, subreddit, type_of_entry):
    if type_of_entry not in ['submission', 'comment']:
        raise ValueError("type_of_entry needs to be submission or comment!")
    
    start_timestamp = int(time.mktime(start_date.timetuple()))
    end_timestamp = int(time.mktime(end_date.timetuple()))
    url = 'https://api.pushshift.io/reddit/search/{}/?subreddit={}&after={}&before={}&metadata=true&size=0'.format(type_of_entry, subreddit, start_timestamp, end_timestamp)
    
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status=5, status_forcelist=[502, 503, 504, 429])
    s.mount('http://', HTTPAdapter(max_retries=retries))
    
    r = s.get(url)
    return int(r.json()['metadata']['total_results'])

@RateLimited(1)
def get_reddit_submissions(subreddit, date):
    """ Gets all submission within the specified timeframe. Result is a dataframe with every entry in its own row.
    """
    start_date = date
    end_date = (date + datetime.timedelta(days=1))
    start_timestamp = int(time.mktime(start_date.timetuple()))
    end_timestamp = int(time.mktime(end_date.timetuple()))

    # prepare retries
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status=5, status_forcelist=[502, 503, 504, 429])
    s.mount('http://', HTTPAdapter(max_retries=retries))

    # for logging purposes check how many entries we expect
    expected_entries = get_expected_number_of_entries(start_date, end_date, subreddit, 'submission')
    number_of_retrieved_entries = 0
    print("Retrieving {} submissions for {}...".format(expected_entries, date.strftime("%Y-%m-%d")))
    
    # the resulting dataframe holding all entries
    df = make_empty_result_df()
    
    # main loop: we only get 100 entries per request so we need dynamically adapt the start_timestamp to get all entries
    running = True
    while running:
        url = 'https://api.pushshift.io/reddit/search/submission/?subreddit={}&size={}&after={}&before={}&sort=asc&sort_type=created_utc'.format(subreddit, 100, start_timestamp, end_timestamp)
        r = s.get(url)
        
        if r.status_code != 200:
            raise ValueError('Failed to get Reddit Submission with code: {}'.format(r.status_code))
        
        number_of_entries_found = len( r.json()['data'] )
        if number_of_entries_found > 0:
            number_of_retrieved_entries += number_of_entries_found
            
            for entry in r.json()['data']:
                if entry['created_utc'] > start_timestamp:
                    start_timestamp = entry['created_utc']
                
                if 'selftext' not in entry:
                    entry['selftext'] = ''

                df = df.append(
                    pd.DataFrame({
                        'Date': pd.to_datetime(date),
                        'Title': str(entry['title']),
                        'Text': str(entry['selftext']),
                        'Comments': pd.to_numeric(entry['num_comments']),
                        'Score': pd.to_numeric(entry['score']),
                        'id': str(entry['id']),
                    }, index=[len(df)] ) )
            print('...retrieved {} of {} entries...'.format(number_of_retrieved_entries, expected_entries))
        else:
            running = False

    print('...done. Found {} submissions for {}.'.format(len(df), date.strftime("%Y-%m-%d")))
    return df

def make_empty_result_df():
    return pd.DataFrame({
                    'Date': np.datetime64(),
                    'Title': str(),
                    'Text': str(),
                    'Comments': int(),
                    'Score': int(),
                    'id': str(),
                    }, index=[])

def store_in_database(data, filename, dupilcate_column=None):
    """ Stores the DataFrame data in file with filename. Duplicates can be identified by a column name.
    """
    # create empty file with correct headers if necessary
    if os.path.exists(filename) is not True:
        make_empty_result_df().to_csv(filename, sep=';', decimal='.', encoding='utf-8', index=False)
    
    # add new entry
    df = pd.read_csv(filename, sep=';', decimal='.', encoding='utf-8', parse_dates=['Date'])
    df = df.append(data).reset_index(drop=True)
    
    # check/remove duplicates in date column
    if dupilcate_column:
        df = df.drop_duplicates(subset=[dupilcate_column], keep='last')

    # save to file
    df.to_csv(filename, sep=';', decimal='.', encoding='utf-8', index=False)

def _fix_cwd():
    """ Makes sure we are in project root, we assume we are two levels deep
    """
    path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(path)
    os.chdir(os.path.pardir)
    os.chdir(os.path.pardir)
    print('Working dir: ', os.getcwd())

def get_all_submissions(dates, subreddit, db_filename):
    """ Gets submissions for all dates of the specified subreddit.

    Returns:
        list of dates: List of dates which could not be retreived.
    """
    retry_list = list()
    for running_date in dates:
        try:
            data = get_reddit_submissions(date=running_date, subreddit=subreddit)
            store_in_database(data, db_filename, dupilcate_column='id')
        except:
            retry_list.append(running_date)
            print('ERROR: failed to get submissions for {} with error {}'.format(running_date.strftime("%Y-%m-%d"), sys.exc_info()))
    return retry_list
        

def main():
    # settings
    start_date = datetime.date(2021, 8, 10)
    end_date = date.today() - timedelta(days=1)
    subreddit = 'wallstreetbets'
    db_filename = 'data/reddit_' + subreddit + '_submissions_2018-2021.csv'
    max_number_retries = 5

    # we assume this code is in /src/analysis while data is in /data. Since we do not want to assume a cwd we switch to src and than move one up
    _fix_cwd()

    # start to work...
    list_of_dates = pd.date_range(start=start_date, end=end_date, freq='D').to_pydatetime()
    
    retries = 0
    working_list = list_of_dates
    while retries < max_number_retries:
        working_list = get_all_submissions(working_list, subreddit, db_filename)
        retries +=1

if __name__ == "__main__":
    main()
