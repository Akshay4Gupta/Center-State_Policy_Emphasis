from pprint import pprint
import os, sys
import pickle
from pymongo import MongoClient
from check import *
art_ids = load(output + filename)

col1, col2 = "health_hygiene", "humanDevelopment"
for name1, val1 in art_ids[col1].items():
    for name2, val2 in art_ids[col2].items():
        inter = val1.intersection(val2)
        if len(inter) > 300:
            print(name1, name2)
            collection1 = db[col2 + "_schemes"]
            for art in inter:
                articles = collection1.find({'_id': art}, no_cursor_timeout=True)
                for article in articles:
                    print(article['text'], end = "\n\n")
