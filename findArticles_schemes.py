#before running it on gem server check the database name.
import pymongo
from pymongo import MongoClient
from collections import Counter
import sys
# ---------- Fixed Params ------------
art_client = MongoClient('mongodb://localhost:27017')
client = MongoClient('mongodb://localhost:27017/')
art_db = art_client['media-db2']
my_db = client['media-db2']
# -----------------------------------
def readfromFile(filename):
    with open('schemes/' + filename + '.txt', 'r+') as file:
        result = ''
        for line in file:
            if line.startswith('#') or line.isspace():
                continue
            if(result != ''):
                result += '|'

            if("yojana" in line.lower()):
                line2 = line.lower().replace("yojana", "scheme") #also change yojana to yojna
                result += line2.strip() + '|'
            result += line.strip()
        return result

print("Finding articles...")
# articles = input('Enter the scheme name: ')
try:
    articles = sys.argv[1]
except:
    print('Enter the scheme name in the argument as: python3 findArticles_schemes.py agriculture(_schemes will be added later)')
    sys.exit()

keywords = readfromFile(articles)
print('KEYWORDS:  ', keywords)

#{'publishedDate':{'$regex' :' 2011 | 2012 | 2013 | 2014-01 |2014-02 | 2014-03 | 2014-04'}},
# Enter the base set of keywords in the regex below, separated by |
x = art_db.articles.find({'$and':[{'text': {'$regex': keywords, '$options': 'i'}}]},
                     no_cursor_timeout=True)

print("Storing articles now...")
coll = my_db[articles + '_schemes']
art_map = {}
cnt = 0

for art in x:
    cnt+=1
    if(cnt%10000 == 0):
        print('Done for '+str(cnt)+' article')
    url = art['articleUrl']

    if url not in art_map:
        art_map[url] = 1
        coll.insert_one(art)
