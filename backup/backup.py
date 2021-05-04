from pymongo import MongoClient
import time
from datetime import datetime
import pickle
import os

picklefile = 'backup_url.pkl'

def save(myvar):
    global picklefile
    with open(picklefile, 'wb') as file:
        pickle.dump(myvar, file)

def load():
    global picklefile
    with open(picklefile, 'rb') as file:
        myvar = pickle.load(file)
        return myvar


# making a database choices
backup_from, backup_to = 'gem2', 'ictd'
collName = 'articles'

# connecting to database
database = {
    'ictd' : ['localhost', 'media-db2'],
    'gem2' : ['10.237.26.159', 'media-db'],
    'gem3' : ['10.208.23.165', 'media-db']
}

backup_from_client  = MongoClient('mongodb://'+ database[backup_from][0] +':27017/')
backup_from_db = backup_from_client[database[backup_from][1]]
backup_from_collection = backup_from_db[collName]

backup_to_client  = MongoClient('mongodb://'+ database[backup_to][0] +':27017/')
backup_to_db = backup_to_client[database[backup_to][1]]
backup_to_collection = backup_to_db[collName]

print('---starting collection find---')
t = datetime.today()
one_month_back = t.strftime('%Y-') + str( int(t.strftime('%m'))-1 ) + t.strftime('-%d')
print(one_month_back)
backup_from_cursor = backup_from_collection.find({'publishedDate':{'$lte':one_month_back}}, no_cursor_timeout=True)
backup_to_cursor = backup_to_collection.find(no_cursor_timeout=True)

url_set = {}
if os.path.isfile(picklefile):
    url_set = load()
else:
    print('---making a set---')
    url_set = set([art['articleUrl'] for art in backup_to_cursor])
    save(url_set)

print('---adding the urls---')
for art in backup_from_cursor:
    url = art['articleUrl']
    if url not in url_set:
        print(art['_id'])
        backup_to_collection.insert_one(art)
        url_set.add(art['articleUrl'])

backup_from_cursor.close()
backup_to_cursor.close()
save(url_set)
