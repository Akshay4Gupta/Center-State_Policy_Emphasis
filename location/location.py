#this script is used to  add two fieds in the articles collection in media-db
# to insert new categories based on keywords go to keyword folder in IndentifyingUsingKeyword and add a <cat>.txt
import pprint
from pymongo import MongoClient
from district_extractor import districtExtractor
from tqdm import tqdm


client = MongoClient('mongodb://localhost:27017')
db = client['media-db2']
collName = input("Enter name of collections: ").strip()
collection=db[collName]



def extractLocation(article):
    global collection
    if isinstance(article['text'], bytes):
        article['text'] = article['text'].decode()
    districtName_code = districtExtractor.districtFinder(article['text'])

    try:
        res = collection.update_one({"_id": article['_id']}, {"$set": {'districtsLocation':districtName_code}})
    except:
        desc='Error while parsing response of article id in extractLocation():>'+str(article['_id'])+'<. Error : '+ str(sys.exc_info()[0])
        with open('ArticlesWtihErrorInDistrict.txt', 'a+') as errorFile:
            print(desc, file = errorFile)




def fetchLoc():
    global collection

    cursor = collection.find({'districtsLocation':{"$exists":False}}).batch_size(50)
    for art in tqdm(cursor):
        extractLocation(art)
    cursor.close()

if __name__ == "__main__":
    fetchLoc()
