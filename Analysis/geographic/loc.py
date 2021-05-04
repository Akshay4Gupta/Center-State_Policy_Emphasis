from pymongo import MongoClient
import re
from elasticsearch import Elasticsearch,helpers
from pprint import pprint
import matplotlib.pyplot as plt
import pickle
from pyjarowinkler.distance import get_jaro_distance
import editdistance
import fuzzy
from tqdm import tqdm


with open("./state_code_state_name", 'rb') as f:
    state_info=pickle.load(f)

state_names = list(map(lambda x: x.lower(), list(state_info.values())))


def findES_Doc(entityName):
    return  {
        "query":
        {

            "match":  { "stdName": entityName }

        }
    }

def pheonetic_distance(name1, name2):

    ''' this returns edit distance for phonetic similarity for two words'''
    soundness1 = fuzzy.nysiis(name1)
    soundness2 = fuzzy.nysiis(name2)
    nysiis_score = editdistance.eval(soundness1, soundness2)

    return nysiis_score, soundness1, soundness2

def match(name1, name2):
    lgth = min(len(name1), len(name2))

    jaro_score = get_jaro_distance(name1, name2)
#         print("Jaro : " , jaro_score)
    editDist_score = editdistance.eval(name1, name2)
#         print("Edit : ", editDist_score)
    sound_score, sond1, sond2 = pheonetic_distance(name1, name2)
#         print("soundness : ", sound_score)
    s_lgth = min(len(sond1), len(sond2))

    if (lgth <= 8  and jaro_score >= 0.94) or (lgth > 8 and  lgth <= 12 and jaro_score >= 0.9) or (lgth > 12 and jaro_score >= 0.87):
#             print("Case 1" )
        return True

    elif (lgth <= 3 and editDist_score < 1) or (lgth > 3 and lgth <= 8 and editDist_score < 2) or (lgth > 8 and editDist_score <= 3):
#             print("Case 2")
        return True

    elif (s_lgth ==0) or (s_lgth <= 3 and sound_score < 1) or (s_lgth > 3 and s_lgth <= 8 and sound_score < 2) or (s_lgth > 8 and sound_score <= 3):
#             print("Case 3")
        return True
    return False


def match_state(cand):
    global state_names
    for state in state_names:
        if state in cand:
            return state
        if match(cand, state):
            return state
    return None

def get_distinct_states(locations):
    dist_states = set()
    for loc in locations:
        loc = loc.lower()
        res = es.search(index=es_index, body=findES_Doc(loc))
        # print(loc, end = ': ')
        hits = res['hits']['hits']
        for i in range(len(hits)):
            candidate = []
            if 'resolutions' in hits[i]['_source'] and hits[i]['_source']['resolutions'] != None and 'containedbycountry' in hits[i]['_source']['resolutions'] and hits[i]['_source']['resolutions']['containedbycountry'] == 'India':
                candidate.append(hits[i]['_source']['resolutions']['containedbystate'])
            if 'aliases' in hits[i]['_source']:
                candidate += hits[i]['_source']['aliases']
            breaked = False
            for cand in candidate:
                state = match_state(cand.lower())
                if state != None:
                    # print(state, end = '')
                    dist_states.add(state)
                    breaked = True
                    break
            if breaked:
                break
        # print()
    return dist_states

def load(filename):
    with open(filename, 'rb') as file:
        return pickle.load(file)


def extractLocation(collection, collName):
    dt_to_state = load('DTCode_to_State')
    cursor = collection.find({"$or":[{'entities':{"$exists":True}}, {'districtsLocation':{"$exists":True}}]}).batch_size(50)
    states = dict()
    for article in tqdm(cursor):
        distinct_states = set()

        if 'entities' in article:
            locations = set()
            for state_entity in article['entities']:
                if state_entity['type'] == 'ProvinceOrState':
                    locations.add(state_entity['name'].upper())
            distinct_states.update(get_distinct_states(locations))

        if 'districtsLocation' in article:
            dist_tuples = article['districtsLocation']
            for dist_tuple in dist_tuples:
                distinct_states.add(dt_to_state[dist_tuple[1]][1].lower())

        for state in distinct_states:
            if state in states:
                states[state] += 1
            else:
                states[state] = 1

    cursor.close()

    return states

# *******************************************************************************************
es = Elasticsearch('10.237.26.117', port=9200, timeout=30)

#create connection to mongo

client = MongoClient('localhost', 27017)
db = client['media-db2']

collName = input("Enter Collection Name: ")
collection = db[collName]

# check if connected to elasticSearch
if es.ping():
    print("connected to ES")
else:
    print("Connection to ES failed")


es_index='index-19apr20-r'

es_mapping='mapping-19apr20-r'
#*************************************************************************************************

# raw_data = es.indices.get_mapping( es_index )
# print ("get_mapping() response type:", type(raw_data))
# pprint(raw_data)

states = extractLocation(collection, collName)

pprint(states)
res = {key: val for key, val in sorted(states.items(), key = lambda ele: ele[0])}
total_sum = sum(res.values())
Percentage = [round((value/total_sum) * 100, 2) for value in res.values()]
st = res.keys()
cnt = res.values()
graph = plt.bar(st, cnt)
i = 0
for p in graph:
    width = p.get_width()
    height = p.get_height()
    x, y = p.get_xy()
    plt.text(x+width/2,
             y+height*1.01,
             str(Percentage[i])+'%',
             ha='center',
             weight='bold', rotation = 90)
    i+=1
plt.xticks(rotation = 90)
plt.title(collName)
plt.tight_layout()
plt.savefig(collName)
plt.clf()


# print(list(states.keys())[156])
# res = es.search(index=es_index, body=findES_Doc(list(states.keys())[156]))
# pprint(res)
