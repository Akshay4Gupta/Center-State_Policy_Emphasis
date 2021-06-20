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

def processArticles(cacheName, art_collection, mp_names, startdate, enddate, typ): 
	if os.path.exists(cacheName+'cache.pkl'):
		with open(cacheName+'cache.pkl', 'rb') as file:
			return pickle.load(file)
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
				# print(MP)
				# related_scheme = article["related_schemes"]
				text = article["text"]
			except Exception as e:
				continue
			sentences = extractor.split_into_sentences(text)
			for name in MP:
				# have to detect if MP/MLA
				state = MP[name][1][1]
				entity_name = MP[name][1][0]
				if entity_name not in mp_names: continue
				party = mp_names[entity_name]
				
				_, byTargetArticles, _ = entitySpecificCoverageAnalysis(sentences, [], name, [name], sNLP)
				if byTargetArticles == []: continue

				for statement in byTargetArticles:
					# print(1, end = '')
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
						sentiment_per_state[state] = {}
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
	# parties = ['NDA', 'UPA', 'Non Aligned']
	# article_collection = db['articles']
	for i in range(total_elections-1):
		year = split_dates[i].split('/')[0]
		df = readExcel.MPs(year, typ)
		for scheme_name in scheme_names:
			art_collection = db[scheme_name + '_schemes']
			MP_names = {}
			for j in range(1, 6):
				names = df[scheme_name][j].to_list()
				for item in names:
					if type(item) != type({}): continue
					MP_names[list(item.keys())[0]] = list(item.values())[0]
			cacheName = 'cache/' + typ + str(year) + scheme_name

			article_sentiments = processArticles(cacheName, art_collection, MP_names, split_dates[i], split_dates[i+1], typ)
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
			
			state_list = df[scheme_name].index.values.tolist()
			# total_articles = no_of_articles(art_collection, state_list, split_dates[i], split_dates[i+1], 'total')
			# state_articles = []
			# for state in state_list:
			# 	state_articles.append(no_of_articles(art_collection, [state], split_dates[i], split_dates[i+1], state))
			# state_articles = np.array(state_articles, dtype = 'float64')
			# state_articles/=total_articles

			# for coll in df[scheme_name]:
			# 	# df[scheme_name][coll] *= total_articles
			# 	# df[scheme_name][coll]/=state_articles
			# 	df[scheme_name][coll] = min_max(df[scheme_name][coll])
			
			# plot
			plt.figure(figsize=(12, 8))
			y_axis = np.zeros((len(state_list), 6), dtype = 'float64')
			width = 0.25
			party_map = {'UPA': 0, 'NDA': 1, 'Non Aligned': 2, 'Third Front': 2, 'Not Aligned': 2}
			for st_id, val in enumerate(article_sentiments.values()):
				for party, emo in val.items():
					for idx, c in enumerate(emo[:-1]):
						# print(party)
						y_axis[st_id][party_map[party]*2 + idx] = c
			x_axis = np.arange(len(state_list))
			plt.bar(x_axis, y_axis[:,0] + y_axis[:,2] + y_axis[:,4], color = 'r', width = width, label = 'negative sentiments')
			plt.bar(x_axis, y_axis[:,1] + y_axis[:,3] + y_axis[:,5], color = 'g', width = width, label = 'positive sentiments')
			plt.title(scheme_name)
			plt.xlabel('States')
			plt.ylabel('Sentiments')
			plt.xticks(x_axis, state_list, rotation=90)
			plt.legend()
			plt.savefig('output/' + typ + scheme_name + str(year) + '.png')
			plt.tight_layout()
			plt.clf()
		
		with open('cache/' + typ + str(year) + 'dataframe.pkl', 'wb') as file:
			pickle.dump(df, file)

		features = pd.concat([df[scheme_name] for scheme_name in scheme_names], axis = 1)
		features = features.fillna(0)
		state_list = features.index.values.tolist()


			
		kmeans = KMeans(n_clusters=6, random_state=5, n_jobs=-1)
		transformer = PCA(n_components = 2, svd_solver = 'randomized')
		
		x_2d = transformer.fit_transform(features)
		prediction = kmeans.fit_predict(x_2d)
		
		plt.figure(figsize=(12, 8))

		colors = ['r', 'b','g', 'orange', 'm', 'y']
		for idx, data in enumerate(x_2d):
		    plt.scatter(data[0], data[1], color=colors[prediction[idx]])
		    
		for idx, data in enumerate(x_2d):
		    plt.annotate(state_list[idx], (data[0], data[1]))
		# plt.legend()
		plt.xlabel("pca-1")
		plt.ylabel("pca-2")
		plt.title("K means clustering for states")
		plt.tight_layout()
		plt.savefig("output/" + typ + str(year))
		plt.clf()