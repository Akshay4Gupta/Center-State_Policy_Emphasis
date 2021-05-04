from pymongo import MongoClient
import re
from elasticsearch import Elasticsearch,helpers
from pprint import pprint
# def load(filename):
#     with open(filename, 'rb') as file:
#         return pickle.load(file)

def findES_Doc(entityName):
#     "from" : 0, "size" : 10,
    return  {
        "size": 10,
        "query":
        {

            "match":  { "stdName": entityName }

        }
    }

def something(collection, collName):
    # dt_to_state = load('DTCode_to_State')
    cursor = collection.find({"$or":[{'entities':{"$exists":True}}, {'districtsLocation':{"$exists":True}}]}).batch_size(50)
    states = dict()
    for article in cursor:
        distinct_states = set()
        if 'entities' in article:
            for state_entity in article['entities']:
                if state_entity['type'] == 'ProvinceOrState':
                    distinct_states.add(state_entity['name'].upper())

            for state in distinct_states:
                if state in states:
                    states[state] += 1
                else:
                    states[state] = 1

    cursor.close()

    return states

if __name__ == '__main__':

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

    states = something(collection, collName)
    print("searching", list(states.keys())[5])
    res = es.search(index=es_index, body=findES_Doc(list(states.keys())[5]))
    pprint(res)
    #
