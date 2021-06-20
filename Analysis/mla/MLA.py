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
import sys

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

party ={
"AAP":"	Aam Aadmi Party	AGP	Asom Gana Parishad",
"AIADMK":"	All India Anna Dravida Munnetra Kazhagam",
"AIFB":"	All India Forward Bloc",
"AIMIM":"	All India Majlis-e-Ittehadul Muslimeen",
"AINRC":"	All India N.R. Congress",
"AITC":"	All India Trinamool Congress",
"AIUDF":"	All India United Democratic Front",
"AJSU":"	All Jharkhand Students Union",
"BJD"	:"Biju Janata Dal",
"BJP"	:"Bharatiya Janata Party",
"BPF"	:"Bodoland People's Front",
"BSP"	:"Bahujan Samaj Party",
"CPI"	:"Communist Party of India",
"CPI-M"	:"Communist Party of India (Marxist)",
"CPI(M)"	:"Communist Party of India (Marxist)",
"DMK"	:"Dravida Munnetra Kazhagam",
"GFP":"	Goa Forward Party",
"HJC"	:"Haryana Janhit Congress",
"HSPDP"	:"Hill State People's Democratic Party",
"INC":"	Indian National Congress",
"INLD":"	Indian National Lok Dal",
"IUML":"	Indian Union Muslim League",
"JD(S)":"	Janata Dal (Secular)",
"JD(U)":"	Janata Dal (United)",
"JKNC":"	Jammu & Kashmir National Conference",
"JKNPP"	:"Jammu & Kashmir National Panthers Party",
"JKPDP"	:"Jammu and Kashmir People's Democratic Party",
"JMM"	:"Jharkhand Mukti Morcha",
"JVM(P)"	:"Jharkhand Vikas Morcha (Prajatantrik)",
"KC(M)"	:"Kerala Congress (M)",
"LJP"	:"Lok Janshakti Party",
"MGP":"	Maharashtrawadi Gomantak Party",
"MNF":"	Mizo National Front",
"MNS":"	Maharashtra Navnirman Sena",
"MPC":"	Mizoram People's Conference",
"MSCP":"	Manipur State Congress Party",
"NCP":"	Nationalist Congress Party",
"NPF":"	Naga People's Front",
"NPP":"	National People's Party",
"PMK"	:"Pattali Makkal Katchi",
"PPA":"	People's Party of Arunachal",
"RJD":"	Rashtriya Janata Dal",
"RLD":"	Rashtriya Lok Dal",
"RLSP"	:"Rashtriya Lok Samta Party",
"RSP"	:"Revolutionary Socialist Party",
"SAD":"	Shiromani Akali Dal",
"SDF":"	Sikkim Democratic Front",
"SJP"	:"Samajwadi Janata Party (Rashtriya)",
"SKM":"	Sikkim Krantikari Morcha",
"SP":"	Samajwadi Party",
"SS":"	Shiv Sena",
"TDP"	:"Telugu Desam Party",
"TRS"	:"Telangana Rashtra Samithi",
"UDP":"	United Democratic Party",
"YSRCP"	:"YSR Congress Party",
"ZNP":"	Zoram Nationalist Party"
}


def find_mlaER(entityName, year):
#     "from" : 0, "size" : 10,

    return  {
      "size" : 20,
      "query": {
        "bool": {
          "should": {
            "range": {
                "Electoral_Info.Position": {
                  "gte": 1.0,
                  "lte": 10.0
                }
              },
            "range": {
                "Electoral_Info.Year": {
                  "gte": year - 6,
                  "lte": year
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


def find_mediaER(entityName):
#     "from" : 0, "size" : 10,
    return  {
      "query": {
          "bool": {
            "should": [
              {
                "match": {
                  "stdName": entityName
                }
              },
              {
                "match": {
                  "aliases": entityName
                }
              }
            ]
          }
      }
    }

def findMlaData(Mla_name, Mla_state):
    return  {
      "query": {
          "bool": {
            "must": [
              {
                "match": {
                  "Name": Mla_name
                }
              },
              {
                "match": {
                  "State": Mla_state
                }
              }
            ]
          }
      }
    }



def isMla(media_er,mla_er, article_states):
    global party
    # pprint(media_er)
    # pprint(mla_er)
    stdName = media_er['stdName'].lower().strip()
    # print('***************** mla names ***********************')
    maxScore = 0
    res = -1

    for idx, mla_info in enumerate(mla_er):


        Score = 0
        mla_name = mla_info['_source']['Name'].lower().strip()
        mla_state = mla_info['_source']['State'].lower().strip()
        if mla_state == 'goa, daman and diu':
            mla_state = 'daman & diu'
        mla_entities = set()
        mla_entities.add(mla_state)

        for info in mla_info['_source']["Electoral_Info"]:
            mla_entities.add(info['Constituency'].lower().strip())
            mla_entities.add(info['Party'].lower().strip())


        for state in article_states:
            if mla_state == state.lower().strip():
                Score = 1
        if stdName != ""  and nameMatch.nameMatch(mla_name,stdName):

            if 'associatedEntities' not in media_er:
                if Score >= 1:
                    # print("here ------------->")
                    return (mla_name, mla_state)
                return None

            else:
                for ent in media_er['associatedEntities']:
                    for mlaEnt in mla_entities:
                        count = ent['count']
                        Ent = ent['text'].strip().lower()
                        Jscore = get_jaro_distance(Ent, mlaEnt)
                        l = min(len(Ent),len(mlaEnt))
                        if Ent.upper() in party:
                            Jscore2 = get_jaro_distance(party[Ent.upper()].strip().lower(), mlaEnt)
                            if Jscore2 > Jscore:
                                l = min(len(party[Ent.upper()].strip().lower()),len(mlaEnt))

                        if ((l  <= 5 and Jscore >= 0.98) or (l > 5 and l <=12 and Jscore >=.95 ) or (l> 12  and Jscore >= .92) ):
                            Score += count

                if Score > maxScore:
                    res = idx
                    maxScore = Score

    if res != -1:
        return (mla_er[res]['_source']['Name'].lower().strip(), mla_er[res]['_source']['State'].lower().strip())
    return None




def findMla(names, states, publishedYear):
    # print(states)
    """
    returns best match for a name and state from MLA ER database
    """
    distinct_names = set()
    for name in names:
        name = name.lower()
        # print('name: ', name)
        media_er = es.search(index=es_index_mediaER, body=find_mediaER(name))
        # pprint(media_er)
        mla_er = es.search(index=es_index_mlaER, body=find_mlaER(name, publishedYear))
        # pprint(mla_er)
        if (len(media_er['hits']['hits']) > 0):
            mla_info = isMla(media_er['hits']['hits'][0]['_source'],mla_er['hits']['hits'], states)

            if mla_info  != None:
                distinct_names.add(mla_info)

    return distinct_names



def extractMla(collection, startDate, endDate):
    cursor=collection.find({'$and':[{'entities':{"$exists":True}},{'publishedDate':{'$gte':startDate}},{'publishedDate':{'$lte':endDate}} ]},no_cursor_timeout=True).batch_size(50)
    # cursor = collection.find({'entities':{"$exists":True}}).batch_size(50)
    state = dict()  # dict of state where each state is dict of mla names with count value
    for article in tqdm(cursor):
        publishedYear = int(article['publishedDate'].split('-')[0])
        name_list = set()
        for per in article['entities']:
            if per['type'].lower() == "person":
                if 'yojana' or 'schemes' not in per['name'].lower():
                    name_list.add(per['name'].lower()) # stores all person mentioned in an article

        # print("names: ", name_list )
        if(len(name_list) < 1):
            continue
        mla_list = findMla(name_list,article['states'], publishedYear)
        # print(mla_list)

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




def toSheet(state, collName, writer):
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
        for idx,mlaName in enumerate(names):
            res = es.search(index=es_index_mlaER, body=findMlaData(mlaName,st))
            for i in range(len(res['hits']['hits'])):
                data[st_idx,idx] = ({"Name ":mlaName}, {'Count ': count[idx]},{"% ":(count[idx]/totalCount)*100} ,{"ElectrolInfo ":res['hits']['hits'][0]['_source']['Electoral_Info']})
                break


    data = np.append(st_name, data, axis=1)

    DF = pd.DataFrame(data)
    print(DF.shape)
    DF.to_excel(writer, sheet_name = collName.split('_')[0]+".xlsx", index = False)




# collNames = ['industrialization_schemes','tourism_culture_schemes','agriculture_schemes', 'environment_schemes', 'health_hygiene_schemes', 'humanDevelopment_schemes']
# collNames = ['environment_schemes']
collNames = ['agriculture_schemes', 'health_hygiene_schemes', 'humanDevelopment_schemes']


startDate, endDate = '2010-12-31', '2021-05-01'
if len(sys.argv) > 1:
    startDate = sys.argv[1]
if len(sys.argv) > 2:
    endDate = sys.argv[2]

print("timeline is : " ,  startDate, " to " , endDate)
writer = pd.ExcelWriter(output+'mla' + startDate.split('-')[0] + '.xlsx', engine='xlsxwriter')
for collName in collNames:
    print('Executing for : ', collName)
    collection = db[collName]
    res = extractMla(collection, startDate, endDate)
    toSheet(res, collName, writer)
writer.save()




"""
Finding MLA ??
1. Associated entities in media ER
2. constituency and party in MLA ER
3. State in meida-db
4. State in MLA ER

5. can also use publishdate of the newssource to filter out the results

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
