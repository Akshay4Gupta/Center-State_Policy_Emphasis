import pickle
import pandas as pd
import csv
import os
import random
from pymongo import MongoClient
from freq import get_schemes

def get_articles_per_schemes(collName, collection, schemes):
    # (str, pymongo.collection.Collection, dict) -> dict
    stats = {}
    for scheme_name, keywords_list in schemes.items():
        keywords = '|'.join(keywords_list)
        scheme_articles = collection.find({'$and':[{'text': {'$regex': keywords, '$options': 'i'}}]},
                            no_cursor_timeout=True)
        N_scheme_articles = collection.find({'$and':[{'text': {'$regex': keywords, '$options': 'i'}}]},
                            no_cursor_timeout=True).count()
        rr = list(set(random.sample(range(0, N_scheme_articles), min(N_scheme_articles, 20))))
        rr.sort()
        stats[scheme_name] = []
        idx = 0
        for i, art in enumerate(scheme_articles):
            if(rr[idx] == i):
                stats[scheme_name].append(art['text'])
                idx+=1
                if idx == len(rr):
                    break
    return stats

if(__name__ == "__main__"):
    schemes_path = 'schemes/'
    schemes_clubbed = get_schemes(schemes_path)

    client = MongoClient('localhost', 27017)
    db = client['media-db2']

    all_stats = {}
    for coll in schemes_clubbed:
        collName = coll + '_schemes'
        collection = db[collName]
        scheme_stat = get_articles_per_schemes(collName, collection, schemes_clubbed[coll])
        all_stats[coll] = scheme_stat


    # with open('articles.pkl', 'wb') as file:
    #         pickle.dump(all_stats, file)
    # articles = load()

    anotation_path = 'anotate/'
    if not os.path.exists(anotation_path):
        os.mkdir(anotation_path)
    for scheme, per_scheme in all_stats.items():
        writer = pd.ExcelWriter(anotation_path+scheme+'.xlsx', engine='xlsxwriter')
        df = [None]*len(per_scheme.keys())
        for i, group_per_scheme in enumerate(per_scheme):
            df[i]= pd.DataFrame(per_scheme[group_per_scheme])
            if(len(group_per_scheme) > 31):
                group_per_scheme = ''.join([name[0] for name in group_per_scheme.split()])
            df[i].to_excel(writer, sheet_name=group_per_scheme)
        writer.save()
