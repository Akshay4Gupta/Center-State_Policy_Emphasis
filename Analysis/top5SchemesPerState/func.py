# import
import argparse
import sys, os, pickle
from pprint import pprint
from datetime import datetime
from tqdm import tqdm, trange
from pymongo import MongoClient
sys.path.append(os.path.abspath("../freq/"))
from freq import get_schemes
from ExtractSentences import ExtractSentences
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from const import *
import numpy as np

# init
now = datetime.now()

client = MongoClient('localhost', 27017)
db = client['media-db2']

dateFormat = '%Y-%m-%d'
parser = argparse.ArgumentParser(description='policy wise analysis',
                                       formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--task', nargs='?', default=task[0], help='=' + str(task))
args = parser.parse_args()

def findSentiment(sentiString):
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(sentiString)
    a_sent = (sentiment["compound"])
    return a_sent

def preprocesstext(doc_set):
    text = doc_set.lower()
    text = text.replace('\r', '')
    text = text.replace('\r\n', '')
    text = text.replace('\n', '')
    text = text.replace('"', '')
    text = text.replace('%', ' ')
    return text

def merge_date_range(state_party_info):
    states = {}
    for _, items in state_party_info.items():
        for state, party_date in items.items():
            if state not in states:
                states[state] = set()
            for party, dateRange in party_date.items():
                states[state].add((party, dateRange[0], \
                    dateRange[1] if len(dateRange) == 2 else now.strftime(dateFormat)))
    for state in states:
        states[state] = list(states[state])
        for idx in range(len(states[state])):
            states[state][idx] = list(states[state][idx])
        states[state].sort(key=lambda x: x[1])
    return states

def split_dates_using_center(center, states):
    for state, state_date_range in states.items():
        center_date_range = center.copy()
        i, j = 0, 0
        n, m = len(center_date_range), len(state_date_range)
        new_list = []
        while(i < n and j < m):
            if center_date_range[i][2] < state_date_range[j][1]:
                i+=1
            elif center_date_range[i][1] > state_date_range[j][2]:
                j+=1
            else:
                # new_list.append([center_date_range[i][0], state_date_range[j][0]])
                if center_date_range[i][1] < state_date_range[j][1]:
                    # new_list[-1].append(state_date_range[j][1])
                    if center_date_range[i][2] < state_date_range[j][2]:
                        new_list.append([center_date_range[i][0], state_date_range[j][0], \
                            state_date_range[j][1], center_date_range[i][2]])
                        # new_list[-1].append(center_date_range[i][2])
                        i+=1
                    else:
                        new_list.append([center_date_range[i][0], state_date_range[j][0], \
                            state_date_range[j][1], state_date_range[j][2]])
                        # new_list[-1].append(state_date_range[j][2])
                        if center_date_range[i][2] == state_date_range[j][2]: i+=1
                        j+=1
                else:
                    if center_date_range[i][2] > state_date_range[j][2]:
                        new_list.append([center_date_range[i][0], state_date_range[j][0], \
                            center_date_range[i][1], state_date_range[j][2]])
                        # new_list[-1].append(state_date_range[j][2])
                        j+=1
                    else:
                        new_list.append([center_date_range[i][0], state_date_range[j][0], \
                            center_date_range[i][1], center_date_range[i][2]])
                        # new_list[-1].append(center_date_range[i][2])
                        if center_date_range[i][2] == state_date_range[j][2]: j+=1
                        i+=1
                        
        states[state] = new_list
    return states

def yearsIn(enddate, startdate):
    # divide by 365.25 to get years
    sd_obj = datetime.strptime(startdate, dateFormat)
    ed_obj = datetime.strptime(enddate, dateFormat)
    return (ed_obj-sd_obj).days

def per_scheme(collection, keywords_list, state, startdate, enddate):
    # print(datetime.strptime(startdate, '%Y-%m-%d').strftime('%d-%m-%y'))
    keywords = '|'.join(keywords_list)
    
    scheme_articles = collection.find({'$and': [ \
        {'text': {'$regex': keywords, '$options': 'i'}}, \
        {'publishedDate': {'$gte': startdate}}, \
        {'publishedDate': {'$lt': enddate}}, \
        {'states': {'$in': [state]}}\
        ]}, no_cursor_timeout = True)
    count = scheme_articles.count()
    return np.array([count, yearsIn(enddate, startdate)])


def sentiments_on_all_statements(collection, scheme_name, keywords_list, state, startdate, enddate):
    global extractor
    keywords = '|'.join(keywords_list)
    scheme_articles = collection.find({'$and': [ \
        {'text': {'$regex': keywords, '$options': 'i'}}, \
        {'publishedDate': {'$gte': startdate}}, \
        {'publishedDate': {'$lte': enddate}}, \
        {'states': {'$in': [state]}}\
        ]}, no_cursor_timeout = True)
    pos_count, neg_count = 0, 0
    for art in scheme_articles:
        text = art['text']
        schemes = art['related_schemes']
        sentences = extractor.split_into_sentences(text)
        sentiment, sc = 0, 0
        for sentence in sentences:
            # for scheme in keywords_list:
                # if scheme not in sentence: continue
                # sentence.replace(scheme, ' ')
                s = findSentiment(sentence)
                if s > 0.5: sentiment+=1
                elif s < -0.5: sentiment-=1
                sc+=1
        if sc > 0:
            if sentiment > 0: pos_count += 1
            else: neg_count += 1
    return np.array([pos_count, neg_count, yearsIn(enddate, startdate)], dtype = 'float64')

def sentiments(collection, scheme_name, keywords_list, state, startdate, enddate):
    global extractor
    keywords = '|'.join(keywords_list)
    scheme_articles = collection.find({'$and': [ \
        {'text': {'$regex': keywords, '$options': 'i'}}, \
        {'publishedDate': {'$gte': startdate}}, \
        {'publishedDate': {'$lte': enddate}}, \
        {'states': {'$in': [state]}}\
        ]}, no_cursor_timeout = True)
    pos_count, neg_count = 0, 0
    for art in scheme_articles:
        text = art['text']
        schemes = art['related_schemes']
        sentences = extractor.split_into_sentences(text)
        sentiment, sc = 0, 0
        for sentence in sentences:
            for scheme in keywords_list:
                if scheme not in sentence: continue
                sentence.replace(scheme, ' ')
                s = findSentiment(sentence)
                if s > 0.5: sentiment+=1
                elif s < -0.5: sentiment-=1
                sc+=1
        if sc > 0:
            if sentiment > 0: pos_count += sentiment/sc
            else: neg_count += sentiment/sc
    return np.array([pos_count, neg_count, yearsIn(enddate, startdate)], dtype = 'float64')

def clean_intervals(center_state_merge):
    for state, party_range in center_state_merge.items():
        total_sessions = len(party_range)
        if total_sessions == 0: continue
        new_party_range = [list(party_range[0])]
        i = 1
        while(i < total_sessions):
            if new_party_range[-1][:2] == list(party_range[i][:2]):
                new_party_range[-1][2] = min([new_party_range[-1][2], party_range[i][2]])
                new_party_range[-1][3] = max([new_party_range[-1][3], party_range[i][3]])
            else:
                new_party_range[-1] = tuple(new_party_range[-1])
                new_party_range.append(list(party_range[i]))
            i+=1
        center_state_merge[state] = new_party_range
    return center_state_merge

def dump(fileName, var):
    with open('cache/'+fileName, 'wb') as file:
        pickle.dump(var, file)

# setting variables
schemes_clubbed = get_schemes(path)
state_party_info = pickle.load(open(inp+'state_party_info.pkl', 'rb'))

extractor = ExtractSentences()
# dateRanges = ['2009-05-22', '2014-05-16', '2019-05-23', now.strftime('%Y-%m-%d')]
# selected_Parties = ['INC', 'BJP', 'BJP']

dateRanges = ['2010-05-22', '2014-05-16', now.strftime('%Y-%m-%d')]
selected_Parties = ['INC', 'BJP']

center_date_range_merged = []
total_elections = len(selected_Parties)
for i in range(total_elections):
    center_date_range_merged.append([selected_Parties[i], dateRanges[i], dateRanges[i+1]])

state_date_range_merged = merge_date_range(state_party_info)

center_state_merge = split_dates_using_center(center_date_range_merged, state_date_range_merged)

center_state_merge = clean_intervals(center_state_merge)

if __name__ == "__main__":
    articles, data = {}, {}
    if args.task == task[0]:
        data = top5Schemes
        for coll in tqdm(data, desc = 'schemes'):
            if coll not in articles:
                articles[coll] = {}
            collection = db[coll+'_schemes']
            for scheme_name in tqdm(data[coll], desc = coll, leave = False): #[MNREGA, PDS, ...]
                keywords_list = schemes_clubbed[coll][scheme_name]
                if scheme_name not in articles[coll]:
                    articles[coll][scheme_name] = {}
                for state, rangeList in tqdm(center_state_merge.items(), desc = scheme_name, leave = False):
                    if state not in articles[coll][scheme_name]:
                        articles[coll][scheme_name][state] = {}
                    for party_range in (rangeList):
                        center_party, state_party, startdate, enddate = party_range
                        key = center_party + ' : ' + state_party
                        if key not in articles[coll][scheme_name][state]:
                            articles[coll][scheme_name][state][key] = np.zeros(2, dtype='float64')
                        articles[coll][scheme_name][state][key] += \
                            per_scheme(collection, keywords_list, state, startdate, enddate)
    elif args.task == task[1]:
        data = schemes_clubbed
        for coll in tqdm(data, desc = 'schemes'): #[agriculture, industrialization, ...]
            if coll not in articles:
                articles[coll] = {}
            collName = coll + '_schemes'
            collection = db[collName]
            keywords_list = []
            for scheme_name in data[coll]: #[MNREGA, PDS, ...]
                keywords_list += schemes_clubbed[coll][scheme_name]
            for state, rangeList in tqdm(center_state_merge.items(), desc = coll, leave = False):
                if state not in articles[coll]:
                    articles[coll][state] = {}
                for party_range in rangeList:
                    center_party, state_party, startdate, enddate = party_range
                    key = center_party + ' : ' + state_party
                    if key not in articles[coll][state]:
                        articles[coll][state][key] = []
                    temp = per_scheme(collection, keywords_list, state, startdate, enddate)
                    # articles[coll][state][key][0] += 
                    articles[coll][state][key].append((temp, startdate, enddate))

    elif args.task == task[2]:
        states = {}
        for coll in tqdm(schemes_clubbed, desc = 'scheme', leave = True):
            collection = db[coll + '_schemes']
            kl = []
            for _, keywords_list in schemes_clubbed[coll].items():
                kl.append('|'.join(keywords_list))
            keywords = '|'.join(kl)
            for state in tqdm(center_state_merge, desc = coll, leave = False):
                if state not in states:
                    states[state] = 0
                states[state] += collection.find({'$and': [ \
                {'text': {'$regex': keywords, '$options': 'i'}}, \
                {'states': {'$in': [state]}}
                ]}).count()
        temp = (sorted(states.items(), key=lambda x: x[0]))
        for state, count in temp:
            print(state, count)
        exit()
    elif args.task == task[3]:
        data = top5Schemes
        for coll in tqdm(data, desc = 'schemes'):
            if coll not in articles:
                articles[coll] = {}
            collection = db[coll+'_schemes']
            for scheme_name in tqdm(data[coll], desc = coll, leave = False): #[MNREGA, PDS, ...]
                keywords_list = schemes_clubbed[coll][scheme_name]
                if scheme_name not in articles[coll]:
                    articles[coll][scheme_name] = {}
                for state, rangeList in center_state_merge.items():
                    for party_range in rangeList:
                        center_party, state_party, startdate, enddate = party_range
                        key = center_party + ' : ' + state_party
                        if key not in articles[coll][scheme_name]:
                            articles[coll][scheme_name][key] = np.zeros(2, dtype='float64')
                        articles[coll][scheme_name][key] += \
                            per_scheme(collection, keywords_list, state, startdate, enddate)
                        articles[coll][scheme_name][key] = yearsIn(enddate, startdate)
    elif args.task == task[4]:
        data = top5Schemes
        for coll in tqdm(data, desc = 'schemes'):
            if coll not in articles:
                articles[coll] = {}
            collection = db[coll+'_schemes']
            for scheme_name in tqdm(data[coll], desc = coll, leave = False): #[MNREGA, PDS, ...]
                keywords_list = schemes_clubbed[coll][scheme_name]
                if scheme_name not in articles[coll]:
                    articles[coll][scheme_name] = {}
                for state, rangeList in tqdm(center_state_merge.items(), desc = scheme_name, leave = False):
                    if state not in articles[coll][scheme_name]:
                        articles[coll][scheme_name][state] = {}
                    for party_range in rangeList:
                        center_party, state_party, startdate, enddate = party_range
                        key = center_party + ' : ' + state_party
                        if key not in articles[coll][scheme_name][state]:
                            articles[coll][scheme_name][state][key] = np.zeros(3, dtype = 'float64')
                        articles[coll][scheme_name][state][key] += sentiments(collection, scheme_name, keywords_list, state, startdate, enddate)
                        
    # pprint(articles)
    dump(args.task, articles)

