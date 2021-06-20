import os, sys, argparse
from pymongo import MongoClient
sys.path.append(os.path.abspath("../freq/"))
from freq import get_schemes
from const import *
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
import numpy as np

now = datetime.now()

output = 'output_graphs/'
client = MongoClient('localhost', 27017)
db = client['media-db2']

schemes_clubbed = get_schemes(path)


Y_axis = []
max_el = 0
for coll, schemes in tqdm(schemes_clubbed.items()):
	collection = db[coll + '_schemes']
	keywords_list = []
	startdate = datetime.strptime('2010-03-03', '%Y-%m-%d')
	for scheme_name, keywords in schemes.items():
		if scheme_name not in top5Schemes[coll]: continue
		keywords_list += keywords
	keywords = '|'.join(keywords_list)
	coll_count = []
	X_axis = []
	while(startdate.year < now.year):
		scheme_articles = collection.find({'$and': [ \
			{'text': {'$regex': keywords, '$options': 'i'}}, \
			{'publishedDate': {'$gte': startdate.strftime('%Y-%m-%d')}}, \
			{'publishedDate': {'$lt': (startdate + relativedelta(years=1)).strftime('%Y-%m-%d')}}, \
			{'sourceName': {'$in': newsSource}}, \
			]}, no_cursor_timeout = True)
		coll_count.append(scheme_articles.count())
		X_axis.append(str(startdate.year) + ' - ' + str((startdate.year+1)%100))
		startdate += relativedelta(years=1)
	coll_count = np.array(coll_count, dtype = 'float64')
	max_el = max(max_el, np.amax(coll_count))
	# coll_count /= np.amax(coll_count)
	Y_axis.append(coll_count)

Y_axis = np.array(Y_axis).T
# Y_axis *= max_el

plt.plot(X_axis, Y_axis)
plt.xticks(rotation = 90)
plt.legend(schemes_clubbed.keys(), bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
plt.tight_layout()
plt.savefig(output + 'articles.png')

