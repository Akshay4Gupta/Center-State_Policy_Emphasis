from const import *
import argparse, sys, os, pickle
from pprint import pprint
sys.path.append(os.path.abspath("../freq/"))
from freq import get_schemes
from tqdm import tqdm
from pymongo import MongoClient
from func import center_state_merge, yearsIn
client = MongoClient('localhost', 27017)
db = client['media-db2']

parser = argparse.ArgumentParser(description='policy wise analysis',
                                       formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--task', nargs='?', default=task[0], help='=' + str(task))
args = parser.parse_args()

with open("cache/"+task[1], 'rb') as f:
	articles = pickle.load(f)

schemes_clubbed = get_schemes(path)

for coll in articles:
	for state in articles[coll]:
		for key in articles[coll][state]:
			cal = (articles[coll][state][key][0][0])
			new_cal = 0
			

# for coll in tqdm(schemes_clubbed, desc = 'schemes'): #[agriculture, industrialization, ...]
#     if coll not in articles:
#         articles[coll] = {}
#     collName = coll + '_schemes'
#     collection = db[collName]
#     keywords_list = []
#     for scheme_name in schemes_clubbed[coll]: #[MNREGA, PDS, ...]
#         keywords_list += schemes_clubbed[coll][scheme_name]
#     for state, rangeList in tqdm(center_state_merge.items(), desc = coll, leave = False):
#         print(keywords_list)
#         break
#     break