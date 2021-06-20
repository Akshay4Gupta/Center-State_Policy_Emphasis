import os, sys, argparse
from pymongo import MongoClient
from datetime import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import numpy as np
from math import ceil
from tqdm import tqdm

sys.path.append(os.path.abspath("../../freq/"))
sys.path.append(os.path.abspath("../"))
from freq import get_schemes
from const import *
from univ import *

now = datetime.now()

parser = argparse.ArgumentParser(description='Extract no. of articles in each category',
                                       formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--startdate', nargs='?', default='2010-05-20', help='date from when you want to process the articles')
parser.add_argument('--enddate', nargs='?', default=now.strftime('%Y-%m-%d'), help='date till when you want to process the articles')
parser.add_argument('--path', nargs='?', default='schemes/', help='path where the schemes to be evaluated are stored')
parser.add_argument('--output', nargs='?', default='output_graphs/v2/National Level/', help='path where the output will be stored')
args = parser.parse_args()
path = args.path
output = args.output

client = MongoClient('localhost', 27017)
db = client['media-db2']

schemes_clubbed = get_schemes(path)


def no_of_articles(collection, keywords_list, startdate, enddate):
    keywords = '|'.join(keywords_list)
    scheme_articles = collection.find({'$and': [ \
        {'text': {'$regex': keywords, '$options': 'i'}}, \
        {'publishedDate': {'$gte': startdate}}, \
        {'publishedDate': {'$lt': enddate}}, \
        {'sourceName': {'$in': newsSource}}, \
        ]}, no_cursor_timeout = True)
    count = scheme_articles.count()
    return count

def no_of_articles_in_scheme(collection, schemes, startdate, enddate):
    keywords_list = []
    for _, keywords in schemes.items():
        keywords_list += keywords
    keywords = '|'.join(keywords_list)
    scheme_articles = collection.find({'$and': [ \
        {'text': {'$regex': keywords, '$options': 'i'}}, \
        {'publishedDate': {'$gte': startdate}}, \
        {'publishedDate': {'$lt': enddate}}, \
        {'sourceName': {'$in': newsSource}}, \
        ]}, no_cursor_timeout = True)
    count = scheme_articles.count()
    return count

print('ploting bar at location: ' + output + '\nstartdate: ' + args.startdate + '\nenddate: ' + args.enddate + '\n')

article_collection = db['articles']


total = {}
for coll, schemes in tqdm(schemes_clubbed.items(), desc = 'schemes'):
    collection = db[coll + '_schemes']
    max_score, c = 0, 0
    fig, ax = plt.subplots(ceil(len(top5Schemes[coll])/2), 2, sharex = True, figsize=(10,10))
    for scheme_name, keywords in tqdm(schemes.items(), desc = coll, leave = False):
        if scheme_name not in top5Schemes[coll]: continue
        startdate = datetime.strptime(args.startdate, '%Y-%m-%d')
        enddate = datetime.strptime(args.enddate, '%Y-%m-%d')
        total_articles, X_axis = [], []
        while(startdate.year < enddate.year):
            count = no_of_articles(collection, keywords, startdate.strftime('%Y-%m-%d'), \
                (startdate + relativedelta(years=1)).strftime('%Y-%m-%d'))
            key = str(startdate.year) + '-' + str((startdate.year+1)%100)
            X_axis.append(key)
            if key not in total:
                # total[key] = no_of_articles_in_scheme(collection, schemes, startdate.strftime('%Y-%m-%d'), \
                #     (startdate + relativedelta(years=1)).strftime('%Y-%m-%d'))
                total[key] = article_collection.find({'$and': [ \
                            {'publishedDate': {'$gte': startdate.strftime('%Y-%m-%d')}}, \
                            {'publishedDate': {'$lt': (startdate + relativedelta(years=1)).strftime('%Y-%m-%d')}}, \
                            {'sourceName': {'$in': newsSource}}, \
                            ]}).count()/10000
                # total[key] = 1
            if total[key] != 0:
                max_score = max(max_score, total[key])
                total_articles.append(count/total[key])
            else:
                total_articles.append(0)
            startdate += relativedelta(years=1)
        total_articles = np.array(total_articles)
        ax[c//2][c%2].plot(X_axis, total_articles*max_score, label=scheme_name)
        ax[c//2][c%2].set_title(scheme_name)
        ax[c//2][c%2].set_xticks([r for r in range(len(X_axis))])
        ax[c//2][c%2].set_xticklabels(X_axis, rotation=45)
        ax[c//2][c%2].axvline(x='2013-14', color='red', linestyle='--')
        ax[c//2][c%2].axvline(x='2018-19', color='red', linestyle='--')
        ax[c//2][c%2].set_xlabel('years')
        ax[c//2][c%2].set_ylabel('Article Count')
        c+=1
    fig.suptitle(coll)
    plt.xticks(rotation=45)
    plt.tight_layout()
    make_dir(output)
    plt.savefig(output + coll+'0.png')
    plt.clf()
    # print(coll)