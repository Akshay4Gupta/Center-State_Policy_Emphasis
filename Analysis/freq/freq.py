# imports
import os
import pickle
from tqdm import tqdm
from pprint import pprint
from pymongo import MongoClient
import matplotlib.pyplot as plt

# initialization
path = 'schemes/'

# definations
def get_schemes(path):
    # type: (str) -> dict
    '''
        returns dict with 5 theme names and in it the dictionary of schemes associated with scheme names
        eg: {
                lifestyle: {
                    'Bima Yojana': ['Rashtriya Swasthya Bima Yojana','RSBY']
                }
            }
    '''
    five_schemes = {}
    for file_path in os.scandir(path):
        # if len(file_path.name.split('_')) == 2:
        #     # to leave out the lifestyle_old file
        #     continue
        with open(file_path.path, 'r+') as file:
            schemes = {}
            keep_a_name = True
            scheme_set_name = ''
            for line in file:
                if line.startswith('#'):
                    scheme_set_name = line.strip()[1:].strip()
                    keep_a_name = False
                    schemes[scheme_set_name] = []
                elif line.isspace():
                    keep_a_name = True
                else:
                    if keep_a_name:
                        scheme_set_name = line.strip()
                        schemes[scheme_set_name] = []
                        keep_a_name = False
                    line = line.strip().lower()
                    schemes[scheme_set_name].append(line)
                    if("yojana" in line):
                        line2 = line.replace("yojana", "scheme")
                        schemes[scheme_set_name].append(line2)
            five_schemes[file_path.name.split('.')[0]] = schemes
    return five_schemes

def num_articles_per_schemes(collName, collection, schemes):
    # (str, pymongo.collection.Collection, dict) -> dict
    stats = {}
    for scheme_name, keywords_list in schemes.items():
        keywords = '|'.join(keywords_list)
        # print(keywords)
        N_scheme_articles = collection.find({'$and':[{'text': {'$regex': keywords, '$options': 'i'}}]},
                            no_cursor_timeout=True).count()
        stats[scheme_name] = N_scheme_articles
    print(stats)
    return stats

def print_stats(all_stats):
    for collName, stats in all_stats.items():
        scm = list(stats.keys())
        scm_name = []
        for scm_nm in scm:
            scm_name.append(scm_nm.replace("pradhan mantri"," ").replace("yojna", " ").replace("schemes"," ").replace("National"," ").replace("Prime Minister"," ")[:70])

        cnt = list(stats.values())
        total_sum = sum(cnt)
        Percentage = [round((value/total_sum) * 100, 2) for value in cnt]
        # scm_name = [i + '  {:.2f}%'.format(j) for i, j in zip(scm_name, Percentage)]
        graph = plt.barh(scm_name, cnt)
        i = 0
        for p in graph:
            percentage =str(Percentage[i])+'%'
            width, height =p.get_width(),p.get_height()
            x=p.get_x()+width+0.02
            y=p.get_y()+height/2
            plt.annotate(percentage,(x,y))
            i+=1
        plt.xticks(rotation = 90)
        plt.title(collName)
        plt.tight_layout()
        output = 'output/'
        if not os.path.exists(output):
            os.mkdir(output)
        plt.savefig(output + collName)
        plt.clf()

if __name__ == '__main__':
    schemes_clubbed = get_schemes(path)

    client = MongoClient('localhost', 27017)
    db = client['media-db2']

    all_stats = {}
    for coll in schemes_clubbed:
        # if coll != 'lifestyle':
        #     continue
        collName = coll + '_schemes'
        collection = db[collName]
        scheme_stat = num_articles_per_schemes(collName, collection, schemes_clubbed[coll])
        all_stats[coll] = scheme_stat
    with open('all_stats.pkl', 'wb') as file:
        pickle.dump(all_stats, file)
    print_stats(all_stats)
