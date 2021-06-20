import os, sys, argparse
from pymongo import MongoClient
from datetime import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import numpy as np
from math import ceil
from tqdm import tqdm
from sklearn.cluster import KMeans
import pickle

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
parser.add_argument('--output', nargs='?', default='output_graphs/', help='path where the output will be stored')
args = parser.parse_args()

def no_of_articles(collection, startdate, enddate, state, keywords_list = []):
	query = {'$and': [ \
		{'publishedDate': {'$gte': startdate.strftime('%Y-%m-%d')}}, \
		{'publishedDate': {'$lt': enddate.strftime('%Y-%m-%d')}}, \
		{'sourceName': {'$in': newsSource}}, \
        {'states': {'$in': [state]}}\
		]}
	if keywords_list:
		keywords = '|'.join(keywords_list)
		query['$and'].append({'text': {'$regex': keywords, '$options': 'i'}})
	count = collection.find(query, no_cursor_timeout = True).count()
	return count

def analyse(article_collection, collection, startdate, enddate, schemes, kmeans):
	states_count = []
	states = large_states+small_states+medium_states
	max_count = 0
	for state in states:
		total_articles = no_of_articles(article_collection, startdate, enddate, state)
		keywords_list = []
		for _, keywords in tqdm(schemes.items(), leave = False):
			keywords_list += keywords
		scheme_articles = no_of_articles(collection, startdate, enddate, state, keywords_list)
		max_count = max(max_count, total_articles)
		states_count.append(scheme_articles/total_articles)
	states_count = np.array(states_count).reshape(-1,1)
	prediction = kmeans.fit_predict(states_count)
	states_divided = [[], [], []]
	for i in range(prediction.size):
	    states_divided[prediction[i]].append(states[i])
	print(states_divided)


def main():

	output = args.output
	kmeans = KMeans(n_clusters=3, random_state=5, n_jobs=-1)

	schemes_clubbed = get_schemes('../schemes')
	enddate = datetime.strptime(args.enddate, '%Y-%m-%d')

	client = MongoClient('localhost', 27017)
	db = client['media-db2']
	article_collection = db['articles']
	
	for coll, schemes in tqdm(schemes_clubbed.items()):
		if(coll != 'humanDevelopment'): continue
		collection = db[coll + '_schemes']
		startdate = datetime.strptime(args.startdate, '%Y-%m-%d')
		while(startdate.year < enddate.year):
			analyse(article_collection, collection, startdate, startdate + relativedelta(years=1), schemes, kmeans)
			break
		break

if __name__ == '__main__':
	main()