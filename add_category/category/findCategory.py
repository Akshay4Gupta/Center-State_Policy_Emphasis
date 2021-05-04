#this script is used to  add two fieds in the articles collection in media-db
# to insert new categories based on keywords go to keyword folder in IndentifyingUsingKeyword and add a <cat>.txt
import sys
try:
    import pprint
    from pymongo import MongoClient
    import os
    path = os.getcwd()+'/IdentifyUsingKeywords'
    sys.path.insert(1, path)
    import IdentifyUsingKeywords
    print("All modules loaded successfully")
except Exception as e:
    print(e)
    sys.exit(0)


client = MongoClient('mongodb://localhost:27017')
db = client['media-db2']
collection_name = input("Enter name of collections: ").strip()
collName = collection_name
collection=db[collName]

def extractCategories(article):
    global collName
    cat = IdentifyUsingKeywords.identifyCategory(article['text'])
    print("category of article found :",cat)
    try:
        rest = collection.update_one({"_id": article['_id']}, {"$set": {'articleCat':cat}})
        # storetext.updateArticle(collName, article['_id'], {'keyword_category':cat})
    except:
        desc='Error while parsing response of article id in extractCategories():>'+str(article['_id'])+'<. Error : '+ str(sys.exc_info()[0])
        print(desc)
        with open('ArticlesWtihErrorInCategories.txt', 'a') as errorFile:
            print(desc, file = errorFile)


def fetchCat_Loc():
    global collection
    cursor = collection.find({'articleCat':{"$exists":False}}).batch_size(50)
    for art in cursor:
        extractCategories(art)
        # extractLocation(art)
    cursor.close()

if __name__ == "__main__":
    fetchCat_Loc()
