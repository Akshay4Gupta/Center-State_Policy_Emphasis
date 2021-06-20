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



es_index_mp="mp_data"

es_mapping_mp="electionData"
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


    return  {
      "size" : 20,
      "query": {
        "bool": {

          "must": [{
            "range": {
                "Electoral_Info.Position": {
                  "gte": 1,
                  "lte": 5
                }
              },
            "range": {
                "Electoral_Info.Year": {
                  "gte": year - 6,
                  "lte": year
                }
              }
          }],

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



def get_alliance(startDate, partyList):
    with open('./coalition', 'rb') as f:
        coalition = pickle.load(f)

    ls_election_year = startDate.split('-')[0]
    nda_alliance = coalition['nda_' + ls_election_year]
    upa_alliance = coalition['upa_' + ls_election_year]
    if ls_election_year == 2014:
        nda_alliance += coalition['nda_2019']
    alliance = "ThirdFront"

    for ele_party in partyList:
        ele_party = ele_party.lower().strip()
        # print(ele_party)
        if ele_party in nda_alliance:
            alliance = "NDA"
            return alliance
        elif ele_party in upa_alliance:
            alliance = "UPA"
            return alliance
    return alliance

def isMp(media_er,mp_db, article_states, startDate):
    global party
    # pprint(media_er)
    # pprint(mp_db)
    stdName = media_er['stdName'].lower().strip()
    # print('***************** mla names ***********************')
    maxScore = 0
    res = -1
    Alliance = []
    for idx, mla_info in enumerate(mp_db):
        alliance = "ThirdFront"
        if mla_info['_source']["Electoral_Info"][0]['Position'] > 5:
            Alliance.append(alliance)
            continue
        Score = 0
        mla_name = mla_info['_source']['Name'].lower().strip()
        mla_state = mla_info['_source']['State'].lower().strip()
        mla_state = mla_state.replace("&amp;", "and")
        if mla_state == 'goa, daman and diu':
            mla_state = 'daman and diu'
        mla_entities = set()
        mla_entities.add(mla_state)

        partyList = []
        for info in mla_info['_source']["Electoral_Info"]:
            mla_entities.add(info['Constituency'].lower().strip())
            if type(info['Party']) == str:
                partyList = [info['Party']]
                mla_entities.add(info['Party'].lower().strip())
            else:
                partyList = [x.lower().strip() for x in info['Party']]
                mla_entities.update(partyList)


        alliance = get_alliance(startDate, partyList)
        for state in article_states:
            if mla_state == state.lower().strip():
                Score = 1
        if stdName != ""  and nameMatch.nameMatch(mla_name,stdName):

            if 'associatedEntities' not in media_er:
                if Score >= 1:
                    # print("here ------------->")
                    return (mla_name, mla_state, alliance)
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
        Alliance.append(alliance)

    if res != -1:
        return (mp_db[res]['_source']['Name'].lower().strip(), mp_db[res]['_source']['State'].lower().strip(), Alliance[res])
    return None




def findMp(names, states, publishedYear, startDate):
    # print(states)
    """
    returns best match for a name and state from MP database
    """
    MP_mapping = dict()
    distinct_names = set()
    for name in names:
        name = name.lower()
        # print('name: ', name)
        media_er = es.search(index=es_index_mediaER, body=find_mediaER(name))
        # pprint(media_er)
        mp_db = es.search(index=es_index_mp, body=find_mlaER(name, publishedYear))
        # pprint(mla_er)
        if (len(media_er['hits']['hits']) > 0):
            mp_info = isMp(media_er['hits']['hits'][0]['_source'],mp_db['hits']['hits'], states, startDate)

            if mp_info  != None:
                Er_name, state, alliance = mp_info
                MP_mapping[name] = [media_er['hits']['hits'][0]['_source']['stdName'].lower().strip(), mp_info]
                distinct_names.add((Er_name, state))

    return distinct_names, MP_mapping



def extractMp(collection, startDate, endDate):
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
        if(len(name_list) < 1 or "states" not in article ):
            continue
        mp_list, MP_mapping = findMp(name_list, article['states'], publishedYear, startDate)
        if(len(mp_list) > 0):
            # Stdname: [media ER name,[politician ER name, state ]]
            rest = collection.update_one({"_id": article['_id']}, {"$set": {'mp':MP_mapping}}, upsert = False)
        # print(mp_list)

        """
        State wise mapping of MP mentioned count
        """

        for mp_info in mp_list:
            mp_nm, st  = mp_info
            if st not in state:
                state[st] = dict()
            if mp_nm not in state[st]:
                state[st][mp_nm] = 1
            else:
                state[st][mp_nm] += 1


    cursor.close()
    for st in state:
        temp = state[st]
        state[st] = {key: val for key, val in sorted(temp.items(), key = lambda ele: ele[1], reverse = True)}
    return state





def toSheet(state, collName, writer, startDate):

    with open('./coalition', 'rb') as f:
        coalition = pickle.load(f)

    ls_election_year = startDate.split('-')[0]
    nda_alliance = coalition['nda_' + ls_election_year]
    upa_alliance = coalition['upa_' + ls_election_year]

    states = list(state.keys())
    st_name = np.empty((35,1), dtype=object)
    cols = ["1", "2", "3", "4", "5", "NDA", "UPA", "ThirdFront"]
    data = np.empty((35,8), dtype=object)
    for st_idx,st in enumerate(states):
        st_name[st_idx] = st
        names = list(state[st].keys())[:5]
        count = list(state[st].values())[:5]
        totalCount = sum(count)
        nda_count, upa_count, non_aligned_count = 0,0,0
        for idx,mlaName in enumerate(names):
            res = es.search(index=es_index_mp, body=findMlaData(mlaName,st))
            for mps in res['hits']['hits']:
                if mps['_source']["Electoral_Info"][0]['Position'] > 5:
                    continue

                electoral_parties = mps['_source']["Electoral_Info"][0]['Party']
                if type(electoral_parties) == str:
                    electoral_parties = [electoral_parties]

                alliance = get_alliance(startDate, electoral_parties)

                if alliance == "NDA":
                    nda_count += 1
                elif alliance == "UPA":
                    upa_count+= 1
                else:
                    non_aligned_count +=1
                ElectrolInfo = mps['_source']['Electoral_Info']
                data[st_idx,idx] = (
                {"Name ":mlaName},
                {'Count ': count[idx]},
                {"% ":(count[idx]/totalCount)*100},
                {"Alliance": alliance} ,
                {"ElectrolInfo ":ElectrolInfo}
                 )
                break
            data[st_idx,5] = nda_count
            data[st_idx,6] = upa_count
            data[st_idx,7] = non_aligned_count

    data = np.append(st_name, data, axis=1)

    DF = pd.DataFrame(data)
    print(DF.shape)
    DF.to_excel(writer, sheet_name = collName.split('_')[0]+".xlsx", index = False)




collNames = ['agriculture_schemes', 'health_hygiene_schemes', 'humanDevelopment_schemes']
# collNames = ['industrialization_schemes','tourism_culture_schemes','agriculture_schemes', 'environment_schemes', 'health_hygiene_schemes', 'humanDevelopment_schemes']
# collNames = ['tourism_culture_schemes']
startDate, endDate = '2009-01-01', '2021-05-01'
if len(sys.argv) > 1:
    startDate = sys.argv[1]
if len(sys.argv) > 2:
    endDate = sys.argv[2]

print("timeline is : " ,  startDate, " to " , endDate)
writer = pd.ExcelWriter(output+'mp' + startDate.split('-')[0] + '.xlsx', engine='xlsxwriter')
for collName in collNames:
    print('Executing for : ', collName)
    collection = db[collName]
    res = extractMp(collection, startDate , endDate)
    toSheet(res, collName, writer, startDate)
writer.save()



"""
Election Dates:

2019 LS = 2019-05-23

2014 LS = 2014-05-16

2009 LS = 2009-05-16
"""


"""
Finding MP ??
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
