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
import numpy as np
import pandas as pd


# initialization
output = 'output/'

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



es_index_mlaER="mla_data"

es_mapping_mlaER="electionData"
#----------------------------------------
es_index_mediaER='index-19apr20-r'

es_mapping_media_ER='mapping-19apr20-r'

#*************************************************************************************************

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
        "bool": {
          "should": {
            "range": {
                "Electoral_Info.Position": {
                  "gte": 1.0,
                  "lte": 10.0
                }
              }
          },
          "must": {
            "bool": {
              "should": [
                {
                  "match": {
                    "Name": entityName
                  }
                },
                {
                  "match": {
                    "Alias": entityName
                  }
                }
              ]
            }
          }
        }
      }
    }
# def findES_Doc(entityName):
# #     "from" : 0, "size" : 10,
#     return  {
#       "query": {
#         "multi_match" : {
#           "query":    entityName,
#           "fields": [ 'Name', 'Alias' ]
#         } ,
#       "bool": {
#               "match": {
#                 "Electoral_Info.Position": 1.0
#               }
#             }
#         }
#     }



def isMLA(names):
    """
    returns best match for a name and state from MLA ER database
    """
    distinct_names = set()
    for name in names:
        name = name.lower()
        res = es.search(index=es_index_mlaER, body=findES_Doc(name))
        # print(name, end = ': ')
        for i in range(len(res['hits']['hits'])):
            if nameMatch.nameMatch(name.lower(),res['hits']['hits'][i]['_source']['Name'].lower()):
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
        mla_list = isMLA(name_list)

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




def tablePlot(state, collName, writer):
    states = list(state.keys())
    st_name = np.empty((35,1), dtype=object)
    cols = [1, 2, 3, 4, 5]
    data = np.empty((35,5), dtype=object)
    for st_idx,st in enumerate(states):
        st_name[st_idx] = st
        names = list(state[st].keys())[:5]
        count = list(state[st].values())[:5]
        totalCount = sum(count)
        mla_info = []
        for idx,name in enumerate(names):
            res = es.search(index=es_index_mlaER, body=findES_Doc(name))
            for i in range(len(res['hits']['hits'])):
                data[st_idx,idx] = ({"Name ":name}, {'Count ': count[idx]},{"% ":(count[idx]/totalCount)*100} ,{"ElectrolInfo ":res['hits']['hits'][0]['_source']['Electoral_Info']})
                break


    data = np.append(st_name, data, axis=1)

    DF = pd.DataFrame(data)
    print(DF.shape)
    DF.to_excel(writer, sheet_name = collName.split('_')[0]+".xlsx", index = False)




# collNames = ['industrialization_schemes','tourism_culture_schemes','agriculture_schemes', 'environment_schemes', 'health_hygiene_schemes', 'humanDevelopment_schemes']
collNames = ['agriculture_schemes']
writer = pd.ExcelWriter(output+'mla.xlsx', engine='xlsxwriter')
for collName in collNames:
    print('Executing for : ', collName)
    collection = db[collName]
    res = extractMla(collection)
    tablePlot(res, collName, writer)
writer.save()




"""
Finding MLA ??
1. Associated entities in media ER
2. constituency and party in MLA ER
3. State in meida-db
4. State in MLA ER

Have only these info to find whether the person found in NER
MLA or not??
"""

"""
boolean queries in Elasticsearch
{
    "query":
    {
        "bool":
        {
            "must":[],
            "filter":[],
            "should":[],
            "must_not":[]
        }
    }
}
"""
