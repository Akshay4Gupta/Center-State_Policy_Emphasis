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
    with open('themes/' + filename + '.txt', 'r+') as file:
        result = ''
        for line in file:
            if(result != ''):
                result += '|'
            result += line.strip()
        return result
print("Finding articles...")
filename = input('Enter the theme name: ')
keywords = readfromFile(filename)
print('KEYWORDS:  ', keywords)
x = art_db.articles.find({'$and':[{'text': {'$regex': keywords, '$options': 'i'}}]},
                     no_cursor_timeout=True)
articles = input('Enter name of collection name to store resultant articles: ')

print("Storing articles now...")
coll = my_db[articles]
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

'''
cnt =0
for art in x1:
    cnt+=1
    print('Done for '+str(cnt)+' article')
    url = art['articleUrl']
    print("art is ",url)
    #break;
    if url not in art_map1:
        art_map1[url] = 1
        coll1.insert_one(art)
        #break;
#print("art map \n",art_map)
#print("art map 2 ", art_map1)
'''
#{'publishedDate':{'$regex' :' 2011 | 2012 | 2013 | 2014-01 |2014-02 | 2014-03 | 2014-04'}},
# Enter the base set of keywords in the regex below, separated by |

#x = art_db.articles.find({'$and':[{'text': {'$regex': ' aadhar | aadhaar | uidai | adhar | adhar card | aadhar card | aadhaar card | pds | public distribution system ','$options': 'i'}},{'$and' :[{'publishedDate':{'$regex' :' 2011|2012|2013|2014-01|2014-02|2014-03|2014-04 '}},{'categories':{'$exists':True}}]}]},no_cursor_timeout=True)
#articles2 = input('Enter name of collection to store resultant articles for after 2014: ')
#print(articles)
#print(x.count())
#print(x1.count())
