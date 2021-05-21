from pprint import pprint
import os, sys
import pickle
from pymongo import MongoClient
sys.path.append(os.path.abspath("../freq/"))
from freq import get_schemes

# initialization
path = 'schemes/'
output = 'output/'
filename = 'backup.pkl'
client = MongoClient('localhost', 27017)
db = client['media-db2']


def diff(set_dict):
    s = set()
    total = 0
    for art in set_dict.values():
        s = s.union(art)
        total += len(art)
    return total, len(s), s

def schemes(collection, schemes):
    # (pymongo.collection.Collection, dict) -> dict
    stats = {}
    for scheme_name, keywords_list in schemes.items():
        keywords = '|'.join(keywords_list)
        scheme_articles = collection.find({'$and':[{'text': {'$regex': keywords, '$options': 'i'}}]},
                            no_cursor_timeout=True)
        stats[scheme_name] = set()
        for art in scheme_articles:
            stats[scheme_name].add(art["_id"])
    return stats

def get_artIDs():
    schemes_clubbed = get_schemes(path)
    all_stats = {}
    for coll in schemes_clubbed:
        collName = coll + '_schemes'
        collection = db[collName]
        all_stats[coll] = schemes(collection, schemes_clubbed[coll])
    return all_stats

def dump(filename, myvar):
    with open(filename, 'wb') as file:
        pickle.dump(myvar, file)

def load(filename):
    with open(filename, 'rb') as file:
        myvar = pickle.load(file)
        return myvar
    
if __name__ == "__main__":    
    if(os.path.exists(output + filename)):
        art_ids = load(output + filename)
    else:
        print("********fetching the articles ID************")
        art_ids = get_artIDs()
        dump(output + filename, art_ids)
    ultimate_match = {}
    for name, ids in art_ids.items():
        total, len_s, ultimate_match[name] = diff(ids)
        #print("Total %d articles i.e. %d%% of articles from %d in %s are intersecting"%(total-len_s, (total-len_s)*100/total, total, name))
        print("%d%% of articles in %s are intersecting"%((total-len_s)*100/total, name))
    total, len_s, inTotalMatch = diff(ultimate_match)
    print()
    print("%d%% of articles in universe are intersecting"%((total-len_s)*100/total))
    print()
    for key1, val1 in ultimate_match.items():
        for key2, val2 in ultimate_match.items():
            if key1 == key2: continue
            dummy = {}
            dummy[key1] = val1
            dummy[key2] = val2
            total, len_s, inTotalMatch = diff(dummy)
            print("Total %d articlesi.e. %d%% of articles from %d in %s and %s are intersecting in universe"%(total-len_s, (total-len_s)*100/total, total, key1, key2))
