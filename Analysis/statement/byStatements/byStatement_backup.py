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

def processArticles(cacheName, art_collection, mp_names, startdate, enddate): 
	if os.path.exists(cacheName+'cache.pkl'):
		with open(cacheName+'cache.pkl', 'rb') as file:
			return pickle.load(file)
	cursor = art_collection.find({'$and': [ \
        {'publishedDate': {'$gte': startdate}}, \
        {'publishedDate': {'$lt': enddate}}, \
        {'sourceName': {'$in': newsSource}}, \
        ]}, no_cursor_timeout = True)
	sentiment_per_state = {}
	sNLP = StanfordNLP()
	try:
		for article in tqdm(cursor):
			try:
				MLA = article["MLA"]
				MP = article["MP"]
				entities = article["entities"]
				related_scheme = article["related_schemes"]
				
				text = article["text"]
				article_id = article["_id"]
				source = article["sourceName"]
			except Exception as e:
				continue
			sentences = []
			names = dict()
			for MP_Name in MP:
				if MP_Name not in mp_names: continue
				x = possibleName(text.lower(), MP_Name.lower())
				if x: 
					for l in x: names[l] = MP_Name
			
			sentences = extractor.split_into_sentences(text)
			possibleNames = set()
			for MP_Name in MP:
				if MP_Name not in mp_names: continue
				for sentence in sentences:
					x = possibleName(sentence.lower(), MP_Name.lower())
					if x: possibleNames.update(x)
			for name in possibleNames:
				# have to detect if MP/MLA
				_, byTargetArticles, _ = entitySpecificCoverageAnalysis(sentences, [], name, [name], sNLP)
				if byTargetArticles == []: continue
				for statement in byTargetArticles:
					entity_name = names[name]
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
					state = MP[entity_name][1][1]
					if state not in sentiment_per_state:
						sentiment_per_state[state] = {}
					party = mp_names[entity_name]
					if party not in sentiment_per_state[state]:
						sentiment_per_state[state][party] = [0, 0, 0]
					if positive > 1 or (positive == 1 and sentiment[0] >= 0.05):
						sentiment_per_state[state][party][1] += 1
					elif negative > 1 or (negative == 1 and sentiment[0] <= -0.05):
						sentiment_per_state[state][party][0] -= 1
					else:
						sentiment_per_state[state][party][2] += 1
	except Exception as e:
		print(e)
		cursor.close()
	return sentiment_per_state

def no_of_articles(collection, states, startdate, enddate, typ):
	path = 'cache/'+typ + '_count.pkl'
	if os.path.exists(path):
		return pickle.load(path)
	scheme_articles = collection.find({'$and': [ \
		{'publishedDate': {'$gte': startdate}}, \
		{'publishedDate': {'$lt': enddate}}, \
		{'sourceName': {'$in': newsSource}}, \
		{'states': {'$in': states}}, \
		]}, no_cursor_timeout = True)
	count = scheme_articles.count()
	with open(path, 'wb') as file:
		pickle.dump(count, file)
	return count

def min_max(v):
    return (v - v.min()) / (v.max() - v.min())

if __name__ == '__main__':
	split_dates = ['2009/05/05', '2014/05/22', '2019/05/22', now.strftime('%Y-%m-%d')]
	total_elections = len(split_dates)
	kmeans = KMeans(n_clusters=5, random_state=5, n_jobs=-1)
	parties = ['NDA', 'UPA', 'Not Aligned']
	article_collection = db['articles']
	for i in range(total_elections-1):
		year = split_dates[i].split('/')[0]
		df = readExcel.MPs(year)
		MP_names = {}
		for scheme_name in scheme_names:
			art_collection = db[scheme_name + '_schemes']
			for j in range(1, 6):
				names = df[scheme_name][j].to_list()
				for item in names:
					if type(item) != type({}): continue
					MP_names[list(item.keys())[0]] = list(item.values())[0]
			cacheName = 'cache/' + str(year) + scheme_name
			article_sentiments = processArticles(cacheName, art_collection, MP_names, split_dates[i], split_dates[i+1])
			if not os.path.exists(cacheName + 'cache.pkl'):
				with open(cacheName + 'cache.pkl', 'wb') as file:
				    pickle.dump(article_sentiments, file)
			for state in article_sentiments:
				for party in article_sentiments[state]:
					if scheme_name + party + '_pos' not in df[scheme_name]: df[scheme_name][scheme_name + party + '_pos'] = 0
					if scheme_name + party + '_neg' not in df[scheme_name]: df[scheme_name][scheme_name + party + '_neg'] = 0
					if scheme_name + party + '_neutral' not in df[scheme_name]: df[scheme_name][scheme_name + party + '_neutral'] = 0
					df[scheme_name].loc[df[scheme_name][0] == state, scheme_name + party + '_pos'] = article_sentiments[state][party][1]
					df[scheme_name].loc[df[scheme_name][0] == state, scheme_name + party + '_neg'] = article_sentiments[state][party][0]
					df[scheme_name].loc[df[scheme_name][0] == state, scheme_name + party + '_neutral'] = article_sentiments[state][party][2]
			df[scheme_name] = df[scheme_name].drop([i for i in range(1, 6)], axis = 1)
			df[scheme_name].rename(columns = {6: scheme_name + 'NDA', 7: scheme_name + 'UPA', 8: scheme_name + 'TF', 0: 'State'}, inplace = True)
			df[scheme_name] = df[scheme_name].set_index('State')
		
		features = pd.concat([df[scheme_name] for scheme_name in scheme_names], axis = 1).dropna()
		with open('cache/'+str(year)+'dataframe.pkl', 'wb') as file:
			pickle.dump(df, file)
		state_list = features.index.values.tolist()

		total_articles = no_of_articles(article_collection, state_list, split_dates[i], split_dates[i+1], 'total')
		state_articles = []
		for state in state_list:
			state_articles.append(no_of_articles(article_collection, [state], split_dates[i], split_dates[i+1], state))
		state_articles = np.array(state_articles, dtype = 'float64')*100/total_articles

		for coll in features:
			features[coll]/=state_articles
			features[coll] = min_max(features[coll])

		prediction = kmeans.fit_predict(features)
			
		transformer = PCA(n_components = 2, svd_solver = 'randomized')
		x_2d = transformer.fit_transform(features)

		plt.figure(figsize=(12, 8))

		colors = ['r', 'b','g', 'orange', 'm', 'y']
		for idx, data in enumerate(x_2d):
		    plt.scatter(data[0], data[1], color=colors[prediction[idx]])
		    
		for idx, data in enumerate(x_2d):
		    plt.annotate(state_list[idx], (data[0], data[1]))
		plt.legend()
		plt.xlabel("pca-1")
		plt.ylabel("pca-2")
		plt.title("K means clustering for states")
		plt.tight_layout()
		plt.savefig("output/"+str(year))
