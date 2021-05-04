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
import os
import nameMatch


state_abbvr = {"uttar pradesh": "UP"
,"bihar": "BH"
,"maharashtra": "MH"
,"madhya pradesh": "MP"
,"tamil nadu": "TN"
,"karnataka":"KA"
,"andhra pradesh":"AP"
,"rajasthan":"RJ"
,"west bengal":"WB"
,"gujarat": "GJ"
,"haryana": "HR"
,"kerala": "KL"
,"assam": "AS"
,"orissa": "OR"
,"punjab": "PJ"
,"delhi": "DL"
,"jharkhand":"JH"
,"jammu & kashmir":"JK"
,"chhattisgarh":"CG"
,"telangana": "TL"
,"uttarakhand":"UK"
,"himachal pradesh":"HP"
,"meghalaya":"ML"
,"manipur":"MN"
,"tripura":"TR"
,"pondicherry":"PY"
,"goa": "GA"
,"mizoram":"MZ"
,"sikkim":"SK"
,"nagaland":"NL"
,"arunachal pradesh":"AR"
,"goa, daman and diu":"DD"}

def findES_Doc(entityName):
#     "from" : 0, "size" : 10,
    return  {
      "query": {
        "multi_match" : {
          "query":    entityName,
          "fields": [ 'Name', 'Alias' ]
        }
      }
    }

def getMLA_name_state(names):
    """
    returns best match for a name and state from MLA ER database
    """
    distinct_names = set()
    for name in names:
        name = name.lower()
        res = es.search(index=es_index, body=findES_Doc(name))
        # print(name, end = ': ')
        for i in range(len(res['hits']['hits'])):
            if nameMatch.nameMatch(name,res['hits']['hits'][i]['_source']['Name']):
                resolvedName = res['hits']['hits'][0]['_source']['Name']
                state_mla = res['hits']['hits'][0]['_source']['State']
                distinct_names.add((resolvedName.lower(), state_mla) )
                break
    return distinct_names

def extractMla(collection):
    cursor = collection.find({'entities':{"$exists":True}}).batch_size(50)
    state = dict()  # dict of state where each state is dict of mla names with count value
    for article in tqdm(cursor):
        name_list = set()
        for per in article['entities']:
            if per['type'] == "Person":
                if 'yojana' or 'schemes' not in per['name'].lower():
                    name_list.add(per['name'].lower()) # stores all person mentioned in an article
        mla_list = getMLA_name_state(name_list)

        """
        State wise mapping of MLA mentioned count
        """

        for mla_info in mla_list:
            mla_nm, st  = mla_info
            if st not in state:
                state[st] = dict()
            if mla_nm not in state[st]:
                state[st][mla_nm] = 1
            else:
                state[st][mla_nm] += 1


    cursor.close()
    for st in state:
        temp = state[st]
        state[st] = {key: val for key, val in sorted(temp.items(), key = lambda ele: ele[1], reverse = True)}
    return state




def tablePlot(state, collName):
    rows = list(state.keys())
    cols = [1, 2, 3, 4, 5]
    data = []
    for st in rows:
        names = list(state[st].keys())[:5]
        names = list(map(lambda x:x.upper(), names))
        while len(names) < 5:
            names.append("")
        data.append(names)

    fig, ax = plt.subplots(figsize=(30,10))
    ax.set_axis_off()
    table = ax.table(
        cellText = data,
        rowLabels = rows,
        colLabels = cols,
        cellLoc ='center',
        loc ='upper left')

    ax.set_title('states x top 5 names',
                 fontweight ="bold")
    output = 'output/'
    if not os.path.exists(output):
        os.mkdir(output)
    plt.savefig(output+collName)
    plt.clf()

# def plotBarGraph(res, collName):
#     global state_abbvr
#     top_n = 21
#     per = list(res.keys())[:top_n]
#     cnt = list(res.values())[:top_n]
#     mla_info = []
#     for per_name in per:
#         per_res =es.search(index=es_index, body=findES_Doc(per_name))
#         st = per_res['hits']['hits'][0]['_source']['State']
#         if st.strip() in state_abbvr:
#             st = state_abbvr[st.strip()]
#         per_name = per_name[:15]
#         mla_info.append(per_name + "_" + st)
#
#
#
#     total_sum = sum(cnt)
#     Percentage = [round((value/total_sum) * 100, 2) for value in cnt]
#     graph = plt.bar(mla_info, cnt)
#     i = 0
#     for p in graph:
#         width = p.get_width()
#         height = p.get_height()
#         x, y = p.get_xy()
#         plt.text(x+width/2,
#                  y+height*1.01,
#                  str(Percentage[i])+'%',
#                  ha='center',
#                  weight='bold', rotation = 90)
#         i+=1
#     plt.xticks(rotation = 90)
#     plt.title(collName)
#     plt.tight_layout()
#     plt.savefig('output/'+collName)
#     plt.clf()


# *******************************************************************************************
es = Elasticsearch('10.237.26.117', port=9200, timeout=30)

#create connection to mongo

client = MongoClient('localhost', 27017)
db = client['media-db2']


# check if connected to elasticSearch
if es.ping():
    print("connected to ES")
else:
    print("Connection to ES failed")

es_index="mla_data"

es_mapping="electionData"

#*************************************************************************************************

collNames = ['industrialization_schemes','tourism_culture_schemes','agriculture_schemes', 'environment_schemes', 'health_hygiene_schemes', 'humanDevelopment_schemes']
# collName = input("Enter Collection Name: ")
# collection = db[collName]
# res = extractMla(collection)
# tablePlot(res, collName)
for collName in collNames:
    print('Executing for : ', collName)
    collection = db[collName]
    res = extractMla(collection)
    tablePlot(res, collName)
