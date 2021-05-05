# imports
import os
import sys
import pickle
from tqdm import tqdm
from pprint import pprint
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
sys.path.append(os.path.abspath("../freq/"))
from freq import get_schemes

# initialization
path = 'schemes/'
output = 'output/'

# definations
def schemes_per_state(collection, schemes):
    # (str, pymongo.collection.Collection, dict) -> dict
    stats = {}
    for scheme_name, keywords_list in schemes.items():
        keywords = '|'.join(keywords_list)
        scheme_articles = collection.find({'$and':[{'text': {'$regex': keywords, '$options': 'i'}}]},
                            no_cursor_timeout=True)
        # scheme_state = {}
        for art in scheme_articles:
            if 'states' in art:
                for state in art['states']:
                    if state not in stats:
                        stats[state] = {}
                    if scheme_name not in stats[state]:
                        stats[state][scheme_name] = 0
                    stats[state][scheme_name] += 1
    return stats

def get_stats():
    backup = 'all_stats.pkl'
    if len(sys.argv) >= 2 and sys.argv[1].lower()[0] == 'n':
        os.remove(output+backup)
    if os.path.exists(output+backup):
        with open(output+backup, 'rb') as file:
            all_stats = pickle.load(file)
            return all_stats
    else:
        schemes_clubbed = get_schemes(path)

        client = MongoClient('localhost', 27017)
        db = client['media-db2']

        all_stats = {}
        for coll in schemes_clubbed:
            collName = coll + '_schemes'
            collection = db[collName]
            all_stats[coll] = schemes_per_state(collection, schemes_clubbed[coll])
        if not os.path.exists(output):
            os.mkdir(output)
        with open(output+backup, 'wb') as file:
            pickle.dump(all_stats, file)
        return all_stats

# main
if __name__ == '__main__':
    print('python3 top5.py new - to process it again')
    all_stats = get_stats()
    writer = pd.ExcelWriter(output+'top5.xlsx', engine='xlsxwriter')
    df = [None]*len(all_stats.keys())
    for i, (scheme, per_scheme) in enumerate(all_stats.items()):
        # print(per_scheme['uttar pradesh'])
        per_scheme = {key: val for key, val in sorted(per_scheme.items(), key = lambda ele: ele[0], reverse = True)}
        sheet = []
        for keys in per_scheme:
            sheet.append([keys])
            per_scheme[keys] = sorted(per_scheme[keys].items(), key = lambda ele: ele[1], reverse = True)[:5]
            for j, x in enumerate(per_scheme[keys]):
                sheet[-1].append(x[0])
            while(len(sheet[-1]) < 6):
                sheet[-1].append("")
        df[i] = pd.DataFrame(sheet, columns = ['State', 1, 2, 3, 4, 5])
        df[i].to_excel(writer, sheet_name=scheme, index = False)
    writer.save()
