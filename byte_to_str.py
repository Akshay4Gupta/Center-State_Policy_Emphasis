from pymongo import MongoClient
from tqdm import tqdm
client = MongoClient('localhost', 27017)
db = client['media-db2']
collName = 'articles'
collection=db[collName]
cursor = collection.find().batch_size(50)
count = 0
for article in tqdm(cursor):
    if isinstance(article['text'], bytes):
        article['text'] = article['text'].decode()
        myquery = { '_id': article['_id'] }
        newvalues = { '$set': { 'text': article['text'] } }
        collection.update_one(myquery, newvalues)
        count += 1
        if(count % 100 == 0):
            print(count)
cursor.close()
