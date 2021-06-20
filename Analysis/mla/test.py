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

def find(Mla_name):
    return  {
      "query": {
          "bool": {
            "must": [
              {
                "match": {
                  "Name": Mla_name
                }
              }

            ]

          }
      }
    }

def findMlaData(mla_id):
    return {
    "query": {
        "bool": {
         "filter": {
                "term": {
                   "_id": mla_id
                }
            }
        }
    }
}

res = es.search(index=es_index_mlaER, body=find("arvind kejriwal"))
print(res)
res = es.search(index=es_index_mlaER, body=findMlaData("s71ci3cB2UrkZGsKnCh6"))
print(res)


# J73TincB2UrkZGsK1RNQ
