from newspaper import Article
from newspaper import ArticleException
from pymongo import MongoClient
# import config
# from storage2 import storetext
import sys
import pprint
# import logging
# import logging.config
from string import Template
from tqdm import tqdm


count = 0
pp = pprint.PrettyPrinter(indent=4)
client = MongoClient('mongodb://127.0.0.1:27017/')
db=client['media-db']
collection=db['test']
collName='test'



def extractRawText(mongo_obj):
    global collection
    global count, collName
    count += 1
    #print('in extract raw text')
    url = mongo_obj['articleUrl']
    #print(url)
    url=url.strip()
    article=Article(url)
    #print(urlJson.sourcename,'  ',urlJson.published_date)
    try:
        article.download()
    except:
        pass
        #storemeta.setBadURL(urlJson.articleid)

    try:
        print(article)
        article.parse() #should throw an exception if article has not been downloaded due to network error or 404
    except ArticleException as ae:
        print('exception:'+ str(ae))
        print('url removed')
        pp.pprint(url)
        return 0

    raw_txt = article.text
    txt = raw_txt.replace('\n',' ')
    txt = txt.strip()
    txt = txt.encode('ascii','ignore')
    raw_txt = raw_txt.encode('ascii','ignore')
    raw_txt = raw_txt.decode('ascii')
    txt = txt.decode('ascii')

    res = collection.update_one({"_id": mongo_obj['_id']}, {"$set": {'rawText':raw_txt, 'text':txt}})
    # storetext.updateArticle(collName,mongo_obj['_id'], {'rawText':raw_txt, 'text':txt})
    return 1
    #print('---------------------------'+str(count)+'-----------------------------')


def fetchUrls(start_date, end_date):
    #print('in fetchUrls')
    global collection
    # start_date = "2019-12-14"
    # end_date = "2020-08-04"
    updated = 0
    cursor = collection.find({'$and':[{'rawText':{'$exists':False}},{'publishedDate':{'$gte':start_date}},{'publishedDate':{'$lte':end_date}}]},  no_cursor_timeout = True).batch_size(100)
    for obj in tqdm(cursor):
        updated +=extractRawText(obj)
        if updated % 1000 == 0:
            print("updated : ", updated)

    cursor.close()

if __name__ == '__main__':
    if len(sys.argv)>2:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = '1900-01-01'
        end_date = '2021-02-28'
    try:
        #while(true):
        fetchUrls(start_date, end_date)
    except:
        print('exception:'+ str(sys.exc_info()[0]))
