# import
import argparse
from pymongo import MongoClient
from ExtractSentences import ExtractSentences
from elasticsearch import Elasticsearch,helpers
from text_parser import StanfordNLP
from utils import *
import os, sys
from tqdm import tqdm
from datetime import datetime
sys.path.append(os.path.abspath("../../top5SchemesPerState/"))
from const import *
import readExcel
from sklearn.cluster import KMeans
import numpy as np
from sklearn.decomposition import PCA
from matplotlib import pyplot as plt
import pickle
from pprint import pprint
import pandas as pd
# ------------------------------------------------------------------------------- #
# disable proxy
os.environ['no_proxy'] = '*'

# ------------------------------------MongoDB------------------------------------------- #

# MongoDB Variables
scheme_names = ['agriculture', 'health_hygiene', 'humanDevelopment']

# Connection To MongoDB
client = MongoClient('localhost', 27017)
db = client['media-db2']


now = datetime.now()
extractor = ExtractSentences()

def processArticles(art_collection, startdate, enddate, typ): 
	cursor = art_collection.find({'$and': [ \
        {'publishedDate': {'$gte': startdate}}, \
        {'publishedDate': {'$lt': enddate}}, \
        ]}, no_cursor_timeout = True)
	sentiment_per_state = {}
	sNLP = StanfordNLP()
	try:
		for article in tqdm(cursor):
			try:
				MP = article[typ]
				text = article["text"]
			except Exception as e:
				continue
			sentences = extractor.split_into_sentences(text)
			for name in MP:
				# have to detect if MP/MLA
				state = MP[name][1][1]
				entity_name = MP[name][1][0]
				
				_, byTargetArticles, _ = entitySpecificCoverageAnalysis(sentences, [], name, [name], sNLP)
				
				for statement in byTargetArticles:
					sentiment = [0, 0, 0]
					sentiment[0] = findSentiment(statement[0])
					sentiment[1] = findSentiment(statement[2])
					sentiment[2] = findSentiment(statement[3])
					
					# findMajority
					positive, negative, neutral = 0, 0, 0
					for i in sentiment:
						if i > 0.05:
							positive+=1
						elif i < -0.05:
							negative+=1
						else: 
							neutral+=1
					if state not in sentiment_per_state:
						sentiment_per_state[state] = [0, 0, 0]
					if positive > 1 or (positive == 1 and sentiment[0] >= 0.05):
						sentiment_per_state[state][1] += 1
					elif negative > 1 or (negative == 1 and sentiment[0] <= -0.05):
						sentiment_per_state[state][0] -= 1
					else:
						sentiment_per_state[state][2] += 1
	except Exception as e:
		print(e)
		cursor.close()
	return sentiment_per_state

def no_of_articles(collection, states, startdate, enddate, typ):
	path = 'cache/'+typ + '_count.pkl'
	# if os.path.exists(path):
	# 	return pickle.load(path)
	scheme_articles = collection.find({'$and': [ \
		{'publishedDate': {'$gte': startdate}}, \
		{'publishedDate': {'$lt': enddate}}, \
		{'sourceName': {'$in': newsSource}}, \
		{'states': {'$in': states}}, \
		]}, no_cursor_timeout = True)
	count = scheme_articles.count()
	# with open(path, 'wb') as file:
	# 	pickle.dump(count, file)
	return count

def min_max(v):
    return (v - v.min()) / (v.max() - v.min())

if __name__ == '__main__':
	split_dates = ['2009/05/05', '2014/05/22', now.strftime('%Y-%m-%d')]
	total_elections = len(split_dates)
	typ = readExcel.args.typ
	for i in range(total_elections-1):
		year = split_dates[i].split('/')[0]
		for scheme_name in scheme_names:
			art_collection = db[scheme_name + '_schemes']
			
			article_sentiments = processArticles(art_collection, split_dates[i], split_dates[i+1], typ)
			
			state_list = list(article_sentiments.keys())
			plt.figure(figsize=(12, 8))
			y_axis = np.zeros((len(state_list), 2), dtype = 'float64')
			width = 0.25
			
			for st_id, val in enumerate(article_sentiments.values()):
				for idx, c in enumerate(val[:-1]):
					y_axis[st_id][idx] = c
			x_axis = np.arange(len(state_list))
			plt.bar(x_axis, y_axis[:,0], color = 'r', width = width, label = 'negative sentiments')
			plt.bar(x_axis, y_axis[:,1], color = 'g', width = width, label = 'positive sentiments')
			plt.title(scheme_name)
			plt.xlabel('States')
			plt.ylabel('Sentiments')
			plt.xticks(x_axis, state_list, rotation=90)
			plt.legend()
			plt.savefig('output/' + typ + scheme_name + str(year) + '_Special.png')
			plt.tight_layout()
			plt.clf()