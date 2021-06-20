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
    cursor = collection.find({'$and':[{'states':{"$exists":False}},{"$or":[{'entities':{"$exists":True}}, {'districtsLocation':{"$exists":True}}]} ]}).batch_size(50)
    for article in tqdm(cursor):
        distinct_states = set()
        old_states = set()
        if 'states' in article:
            old_states.update(article['states'])
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

        # if(old_states != distinct_states):
        #     states_in_article = list(distinct_states)
        #     if(not old_states.issubset(distinct_states)):
        #         print(states_in_article)
        #         distinct_states.update(old_states)


        states_in_article = list(distinct_states)
        # print(states_in_article)
        rest = collection.update_one({"_id": article['_id']}, {"$set": {'states':states_in_article}}, upsert = False)
    cursor.close()


# *******************************************************************************************
es = Elasticsearch('10.237.26.117', port=9200, timeout=30)

#create connection to mongo

client = MongoClient('localhost', 27017)
db = client['media-db2']

# collName = input("Enter Collection Name: ")
# allCollName = ['industrialization','environment','humanDevelopment','tourism_culture','health_hygiene', 'agriculture']
allCollName = ['humanDevelopment','health_hygiene', 'agriculture']



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

for collName in allCollName:
    collection = db[collName+'_schemes']
    print(collName)
    extractLocation(collection, collName)




# { "_id" : ObjectId("58caea33a3d24b23204a5d32"), "category" : "REGIONAL_NEWS", "publishedTime" : null, "language" : "English", "publishedDate" : "2014-01-02", "sourceName" : "The Hindu", "country" : "India", "author" : [ "Staff Correspondent" ], "text" : "RuPay debit cards will be issued for the farmers, who are members of cooperative societies, in the district by April, said M.S. Raghavendra, Assistant General Manager of National Bank for Agriculture and Rural Development (NABARD).He was speaking after inaugurating a training programme held in the city on Wednesday for executive officers of Primary Agricultural Cooperative Societies (PACS) on conducting financial transactions on RuPay platform of National Payments Corporation of India.The ongoing process of computerisation of PACS is likely to be completed by the end of March. After the completion of the computerisation process, the PACS will switch over to core banking system.This system will enable the PACS to operate on RuPay platform.After the completion of the computerisation process, RuPay Kisan credit cards will be distributed among the farmers who are members of cooperative societies, he said.R.M. Manjunatha Gowda, President of Karnataka State Cooperative Apex Bank and Shimoga District Central Cooperative Bank said that the RuPay debit cards were user-friendly.After the completion of computerisation process, the PACS will emerge as multi-service providing centres.In future, the subsidy of various government schemes will be directly remitted to the accounts of the farmers in cooperative societies, Mr. Raghavendra said.", "articleTitle" : "RuPay debit cards for Shimoga farmers in April", "articleUrl" : "http://www.thehindu.com/todays-paper/tp-national/tp-karnataka/rupay-debit-cards-for-shimoga-farmers-in-april/article5529053.ece", "socialTags" : [ { "importance" : "2", "forenduserdisplay" : "true", "type" : "socialTag", "name" : "Cooperative banking", "originalvalue" : null }, { "importance" : "2", "forenduserdisplay" : "true", "type" : "socialTag", "name" : "Debit card", "originalvalue" : null }, { "importance" : "2", "forenduserdisplay" : "true", "type" : "socialTag", "name" : "National Bank for Agriculture and Rural Development", "originalvalue" : null }, { "importance" : "2", "forenduserdisplay" : "true", "type" : "socialTag", "name" : "Payment systems", "originalvalue" : null }, { "importance" : "2", "forenduserdisplay" : "true", "type" : "socialTag", "name" : "E-commerce", "originalvalue" : null }, { "importance" : "2", "forenduserdisplay" : "true", "type" : "socialTag", "name" : "National Payments Corporation of India", "originalvalue" : null }, { "importance" : "2", "forenduserdisplay" : "true", "type" : "socialTag", "name" : "Interbank networks", "originalvalue" : null }, { "importance" : "2", "forenduserdisplay" : "true", "type" : "socialTag", "name" : "RuPay", "originalvalue" : null }, { "importance" : "2", "forenduserdisplay" : "true", "type" : "socialTag", "name" : "Banking in India", "originalvalue" : null }, { "importance" : "1", "forenduserdisplay" : "true", "type" : "socialTag", "name" : "Money", "originalvalue" : null }, { "importance" : "1", "forenduserdisplay" : "true", "type" : "socialTag", "name" : "Financial services", "originalvalue" : null }, { "importance" : "1", "forenduserdisplay" : "true", "type" : "socialTag", "name" : "Finance", "originalvalue" : null } ], "entities" : [ { "name" : "Assistant General Manager", "type" : "Position", "instances" : [ { "suffix" : " of National Bank for Agriculture and Rural", "prefix" : " in the district by April, said M.S. Raghavendra, ", "detection" : "[ in the district by April, said M.S. Raghavendra, ]Assistant General Manager[ of National Bank for Agriculture and Rural]", "length" : 25, "offset" : 141, "exact" : "Assistant General Manager" } ], "relevance" : 0.2, "forenduserdisplay" : "false", "aliases" : [ "Assistant General Manager" ] }, { "name" : "M.S. Raghavendra", "type" : "Person", "instances" : [ { "suffix" : ", Assistant General Manager of National Bank for", "prefix" : "societies, in the district by April, said ", "detection" : "[societies, in the district by April, said ]M.S. Raghavendra[, Assistant General Manager of National Bank for]", "length" : 16, "offset" : 123, "exact" : "M.S. Raghavendra" }, { "suffix" : " was speaking after inaugurating a training", "prefix" : "for Agriculture and Rural Development (NABARD).", "detection" : "[for Agriculture and Rural Development (NABARD).]He[ was speaking after inaugurating a training]", "length" : 2, "offset" : 231, "exact" : "He" }, { "suffix" : " said.R.M. Manjunatha Gowda, President of", "prefix" : "who are members of cooperative societies, ", "detection" : "[who are members of cooperative societies, ]he[ said.R.M. Manjunatha Gowda, President of]", "length" : 2, "offset" : 906, "exact" : "he" }, { "suffix" : " said.", "prefix" : "of the farmers in cooperative societies, ", "detection" : "[of the farmers in cooperative societies, ]Mr. Raghavendra[ said.]", "length" : 15, "offset" : 1326, "exact" : "Mr. Raghavendra" } ], "relevance" : 0.8, "resolutions" : null, "forenduserdisplay" : "true", "aliases" : [ "M.S. Raghavendra" ] }, { "name" : "President", "type" : "Position", "instances" : [ { "suffix" : " of Karnataka State Cooperative Apex Bank and", "prefix" : "societies, he said.R.M. Manjunatha Gowda, ", "detection" : "[societies, he said.R.M. Manjunatha Gowda, ]President[ of Karnataka State Cooperative Apex Bank and]", "length" : 9, "offset" : 937, "exact" : "President" } ], "relevance" : 0.2, "forenduserdisplay" : "false", "aliases" : [ "President" ] }, { "name" : "R.M. Manjunatha Gowda", "type" : "Person", "instances" : [ { "suffix" : ", President of Karnataka State Cooperative Apex", "prefix" : "are members of cooperative societies, he said.", "detection" : "[are members of cooperative societies, he said.]R.M. Manjunatha Gowda[, President of Karnataka State Cooperative Apex]", "length" : 21, "offset" : 914, "exact" : "R.M. Manjunatha Gowda" } ], "relevance" : 0.2, "resolutions" : null, "forenduserdisplay" : "true", "aliases" : [ "R.M. Manjunatha Gowda" ] }, { "name" : "National Bank", "type" : "Company", "instances" : [ { "suffix" : " for Agriculture and Rural Development", "prefix" : "M.S. Raghavendra, Assistant General Manager of ", "detection" : "[M.S. Raghavendra, Assistant General Manager of ]National Bank[ for Agriculture and Rural Development]", "length" : 13, "offset" : 170, "exact" : "National Bank" } ], "relevance" : 0.8, "resolutions" : null, "forenduserdisplay" : "false", "aliases" : [ "National Bank" ] }, { "name" : "Karnataka State Cooperative Apex Bank", "type" : "Company", "instances" : [ { "suffix" : " and Shimoga District Central Cooperative Bank", "prefix" : "he said.R.M. Manjunatha Gowda, President of ", "detection" : "[he said.R.M. Manjunatha Gowda, President of ]Karnataka State Cooperative Apex Bank[ and Shimoga District Central Cooperative Bank]", "length" : 37, "offset" : 950, "exact" : "Karnataka State Cooperative Apex Bank" }, { "suffix" : " and Shimoga District Central Cooperative Bank", "prefix" : "he said.R.M. Manjunatha Gowda, President of ", "detection" : "[he said.R.M. Manjunatha Gowda, President of ]Karnataka State Cooperative Apex Bank[ and Shimoga District Central Cooperative Bank]", "length" : 37, "offset" : 950, "exact" : "Karnataka State Cooperative Apex Bank" } ], "relevance" : 0.2, "resolutions" : null, "forenduserdisplay" : "false", "aliases" : [ "Karnataka State Cooperative Apex Bank" ] }, { "name" : "national payments corporation of india", "type" : "Company", "instances" : [ { "suffix" : ".The ongoing process of computerisation of PACS", "prefix" : "financial transactions on RuPay platform of ", "detection" : "[financial transactions on RuPay platform of ]National Payments Corporation of India[.The ongoing process of computerisation of PACS]", "length" : 38, "offset" : 451, "exact" : "National Payments Corporation of India" } ], "relevance" : 0.8, "resolutions" : [ { "name" : "National Payments Corporation of India", "permid" : "5037350705", "ispublic" : "false", "commonname" : "NPCI", "score" : 0.041505087, "id" : "https:\\/\\/permid.org\\/1-5037350705" } ], "forenduserdisplay" : "true", "aliases" : [ "national payments corporation of india" ] }, { "name" : "Shimoga District Central Cooperative Bank", "type" : "Company", "instances" : [ { "suffix" : " said that the RuPay debit cards were", "prefix" : "of Karnataka State Cooperative Apex Bank and ", "detection" : "[of Karnataka State Cooperative Apex Bank and ]Shimoga District Central Cooperative Bank[ said that the RuPay debit cards were]", "length" : 41, "offset" : 992, "exact" : "Shimoga District Central Cooperative Bank" }, { "suffix" : " said that the RuPay debit cards were", "prefix" : "of Karnataka State Cooperative Apex Bank and ", "detection" : "[of Karnataka State Cooperative Apex Bank and ]Shimoga District Central Cooperative Bank[ said that the RuPay debit cards were]", "length" : 41, "offset" : 992, "exact" : "Shimoga District Central Cooperative Bank" } ], "relevance" : 0.2, "resolutions" : null, "forenduserdisplay" : "true", "aliases" : [ "Shimoga District Central Cooperative Bank" ] } ], "extracted" : true, "keywords" : [ { "relevance" : 22.333333333333336, "text" : "karnataka state cooperative apex bank" }, { "relevance" : 20.333333333333332, "text" : "shimoga district central cooperative bank" }, { "relevance" : 14.133333333333335, "text" : "rupay kisan credit cards" }, { "relevance" : 13.833333333333334, "text" : "primary agricultural cooperative societies" }, { "relevance" : 9.133333333333333, "text" : "rupay debit cards" }, { "relevance" : 9, "text" : "service providing centres" }, { "relevance" : 9, "text" : "assistant general manager" }, { "relevance" : 9, "text" : "training programme held" }, { "relevance" : 9, "text" : "conducting financial transactions" }, { "relevance" : 8.5, "text" : "national payments corporation" }, { "relevance" : 8, "text" : "core banking system" }, { "relevance" : 6.5, "text" : "national bank" }, { "relevance" : 5.833333333333334, "text" : "cooperative societies" }, { "relevance" : 4.8, "text" : "rupay platform" }, { "relevance" : 4, "text" : "ongoing process" }, { "relevance" : 4, "text" : "government schemes" }, { "relevance" : 4, "text" : "manjunatha gowda" }, { "relevance" : 4, "text" : "directly remitted" }, { "relevance" : 4, "text" : "rural development" }, { "relevance" : 4, "text" : "executive officers" }, { "relevance" : 3.75, "text" : "computerisation process" }, { "relevance" : 3, "text" : "district" }, { "relevance" : 2, "text" : "system" }, { "relevance" : 1.75, "text" : "computerisation" }, { "relevance" : 1, "text" : "inaugurating" }, { "relevance" : 1, "text" : "raghavendra" }, { "relevance" : 1, "text" : "members" }, { "relevance" : 1, "text" : "issued" }, { "relevance" : 1, "text" : "india" }, { "relevance" : 1, "text" : "accounts" }, { "relevance" : 1, "text" : "nabard" }, { "relevance" : 1, "text" : "completion" }, { "relevance" : 1, "text" : "city" }, { "relevance" : 1, "text" : "end" }, { "relevance" : 1, "text" : "farmers" }, { "relevance" : 1, "text" : "agriculture" }, { "relevance" : 1, "text" : "distributed" }, { "relevance" : 1, "text" : "switch" }, { "relevance" : 1, "text" : "subsidy" }, { "relevance" : 1, "text" : "speaking" }, { "relevance" : 1, "text" : "enable" }, { "relevance" : 1, "text" : "march" }, { "relevance" : 1, "text" : "completed" }, { "relevance" : 1, "text" : "user" }, { "relevance" : 1, "text" : "president" }, { "relevance" : 1, "text" : "multi" }, { "relevance" : 1, "text" : "pacs" }, { "relevance" : 1, "text" : "emerge" }, { "relevance" : 1, "text" : "wednesday" }, { "relevance" : 1, "text" : "friendly" }, { "relevance" : 1, "text" : "april" }, { "relevance" : 1, "text" : "future" }, { "relevance" : 1, "text" : "operate" } ], "sentiStrength" : "1", "topics" : [ { "score" : 1, "type" : "topics", "name" : "Business_Finance" } ], "taggedText" : "RuPay debit cards will be issued for the farmers, who are members of cooperative societies, in the district by April, said <PER>203816</PER>, Assistant General Manager of National Bank for Agriculture and Rural Development (NABARD).He was speaking after inaugurating a training programme held in the city on Wednesday for executive officers of Primary Agricultural Cooperative Societies (PACS) on conducting financial transactions on RuPay platform of National Payments Corporation of India.The ongoing process of computerisation of PACS is likely to be completed by the end of March. After the completion of the computerisation process, the PACS will switch over to core banking system.This system will enable the PACS to operate on RuPay platform.After the completion of the computerisation process, RuPay Kisan credit cards will be distributed among the farmers who are members of cooperative societies, he said.<PER>192520</PER>, President of Karnataka State Cooperative Apex Bank and Shimoga District Central Cooperative Bank said that the RuPay debit cards were user-friendly.After the completion of computerisation process, the PACS will emerge as multi-service providing centres.In future, the subsidy of various government schemes will be directly remitted to the accounts of the farmers in cooperative societies, Mr. Raghavendra said.", "newEntities" : [ { "name" : "Assistant General Manager", "type" : "Position", "instances" : [ { "suffix" : " of National Bank for Agriculture and Rural", "prefix" : " in the district by April, said M.S. Raghavendra, ", "detection" : "[ in the district by April, said M.S. Raghavendra, ]Assistant General Manager[ of National Bank for Agriculture and Rural]", "length" : 25, "offset" : 142, "exact" : "Assistant General Manager" } ], "relevance" : 0.2, "forenduserdisplay" : "false", "aliases" : [ "Assistant General Manager" ] }, { "name" : "President", "type" : "Position", "instances" : [ { "suffix" : " of Karnataka State Cooperative Apex Bank and", "prefix" : "societies, he said.R.M. Manjunatha Gowda, ", "detection" : "[societies, he said.R.M. Manjunatha Gowda, ]President[ of Karnataka State Cooperative Apex Bank and]", "length" : 9, "offset" : 934, "exact" : "President" } ], "relevance" : 0.2, "forenduserdisplay" : "false", "aliases" : [ "President" ] }, { "name" : "National Bank", "type" : "Company", "instances" : [ { "suffix" : " for Agriculture and Rural Development", "prefix" : "M.S. Raghavendra, Assistant General Manager of ", "detection" : "[M.S. Raghavendra, Assistant General Manager of ]National Bank[ for Agriculture and Rural Development]", "length" : 13, "offset" : 171, "exact" : "National Bank" } ], "relevance" : 0.8, "resolutions" : null, "forenduserdisplay" : "false", "aliases" : [ "National Bank" ] }, { "name" : "Karnataka State Cooperative Apex Bank", "type" : "Company", "instances" : [ { "suffix" : " and Shimoga District Central Cooperative Bank", "prefix" : "he said.R.M. Manjunatha Gowda, President of ", "detection" : "[he said.R.M. Manjunatha Gowda, President of ]Karnataka State Cooperative Apex Bank[ and Shimoga District Central Cooperative Bank]", "length" : 37, "offset" : 947, "exact" : "Karnataka State Cooperative Apex Bank" }, { "suffix" : " and Shimoga District Central Cooperative Bank", "prefix" : "he said.R.M. Manjunatha Gowda, President of ", "detection" : "[he said.R.M. Manjunatha Gowda, President of ]Karnataka State Cooperative Apex Bank[ and Shimoga District Central Cooperative Bank]", "length" : 37, "offset" : 947, "exact" : "Karnataka State Cooperative Apex Bank" } ], "relevance" : 0.2, "resolutions" : null, "forenduserdisplay" : "false", "aliases" : [ "Karnataka State Cooperative Apex Bank" ] }, { "name" : "national payments corporation of india", "type" : "Company", "instances" : [ { "suffix" : ".The ongoing process of computerisation of PACS", "prefix" : "financial transactions on RuPay platform of ", "detection" : "[financial transactions on RuPay platform of ]National Payments Corporation of India[.The ongoing process of computerisation of PACS]", "length" : 38, "offset" : 452, "exact" : "National Payments Corporation of India" } ], "relevance" : 0.8, "resolutions" : [ { "name" : "National Payments Corporation of India", "permid" : "5037350705", "ispublic" : "false", "commonname" : "NPCI", "score" : 0.041505087, "id" : "https:\\/\\/permid.org\\/1-5037350705" } ], "forenduserdisplay" : "true", "aliases" : [ "national payments corporation of india" ] }, { "name" : "Shimoga District Central Cooperative Bank", "type" : "Company", "instances" : [ { "suffix" : " said that the RuPay debit cards were", "prefix" : "of Karnataka State Cooperative Apex Bank and ", "detection" : "[of Karnataka State Cooperative Apex Bank and ]Shimoga District Central Cooperative Bank[ said that the RuPay debit cards were]", "length" : 41, "offset" : 989, "exact" : "Shimoga District Central Cooperative Bank" }, { "suffix" : " said that the RuPay debit cards were", "prefix" : "of Karnataka State Cooperative Apex Bank and ", "detection" : "[of Karnataka State Cooperative Apex Bank and ]Shimoga District Central Cooperative Bank[ said that the RuPay debit cards were]", "length" : 41, "offset" : 989, "exact" : "Shimoga District Central Cooperative Bank" } ], "relevance" : 0.2, "resolutions" : null, "forenduserdisplay" : "true", "aliases" : [ "Shimoga District Central Cooperative Bank" ] } ], "updateTag" : true, "taggedTextM" : "RuPay debit cards will be issued for the farmers, who are members of cooperative societies, in the district by April, said <PER>5982d0869855b7221e1847c0</PER>, Assistant General Manager of <ORG>5982d06d9855b7221e183435</ORG> for Agriculture and Rural Development (NABARD).He was speaking after inaugurating a training programme held in the city on Wednesday for executive officers of Primary Agricultural Cooperative Societies (PACS) on conducting financial transactions on RuPay platform of <ORG>5982d0659855b7221e182d47</ORG>.The ongoing process of computerisation of PACS is likely to be completed by the end of March. After the completion of the computerisation process, the PACS will switch over to core banking system.This system will enable the PACS to operate on RuPay platform.After the completion of the computerisation process, RuPay Kisan credit cards will be distributed among the farmers who are members of cooperative societies, he said.<PER>5982d0a19855b7221e185f54</PER>, President of <ORG>5982d1bf9855b7221e1948b4</ORG> and <ORG>5982d1d79855b7221e195d35</ORG> said that the RuPay debit cards were user-friendly.After the completion of computerisation process, the PACS will emerge as multi-service providing centres.In future, the subsidy of various government schemes will be directly remitted to the accounts of the farmers in cooperative societies, Mr. Raghavendra said.", "newEntitiesM" : [ { "name" : "Assistant General Manager", "type" : "Position", "instances" : [ { "suffix" : " of National Bank for Agriculture and Rural", "prefix" : " in the district by April, said M.S. Raghavendra, ", "detection" : "[ in the district by April, said M.S. Raghavendra, ]Assistant General Manager[ of National Bank for Agriculture and Rural]", "length" : 25, "offset" : 160, "exact" : "Assistant General Manager" } ], "relevance" : 0.2, "forenduserdisplay" : "false", "aliases" : [ "Assistant General Manager" ] }, { "name" : "President", "type" : "Position", "instances" : [ { "suffix" : " of Karnataka State Cooperative Apex Bank and", "prefix" : "societies, he said.R.M. Manjunatha Gowda, ", "detection" : "[societies, he said.R.M. Manjunatha Gowda, ]President[ of Karnataka State Cooperative Apex Bank and]", "length" : 9, "offset" : 989, "exact" : "President" } ], "relevance" : 0.2, "forenduserdisplay" : "false", "aliases" : [ "President" ] } ], "updateTagM" : true, "districtsLocation" : [ [ "Shimoga", 568 ], [ "Central", 95 ] ], "states" : [ "nct of delhi", "karnataka" ], "related_schemes" : [ "kisan credit card", "kisan credit cards" ], "MLA" : { "r.m. manjunatha gowda" : [ "r.m. manjunatha gowda", [ "r. m. manjunatha gowda", "karnataka" ] ], "m.s. raghavendra" : [ "m.s. raghavendra", [ "raghavendra", "karnataka" ] ] }, "mla" : { "m.s. raghavendra" : [ "m.s. raghavendra", [ "raghavendra", "karnataka" ] ], "r.m. manjunatha gowda" : [ "r.m. manjunatha gowda", [ "r. m. manjunatha gowda", "karnataka" ] ] } }
