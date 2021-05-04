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



def findES_Doc(entityName):
#     "from" : 0, "size" : 10,
    return  {
      "query": {
        "multi_match" : {
          "query":    entityName,
          "fields": [ 'stdName', 'aliases' ]
        }
      }
    }


def pheonetic_distance(name1, name2):

    ''' this returns edit distance for phonetic similarity for two words'''
    soundness1 = fuzzy.nysiis(name1)
    soundness2 = fuzzy.nysiis(name2)
    nysiis_score = editdistance.eval(soundness1, soundness2)

    return nysiis_score, soundness1, soundness2


def sort_words(words):
    words = words.split(" ")
    words.sort()
    newSentence = " ".join(words)
    return newSentence

def isSubSequence(str1,str2):
    m = len(str1)
    n = len(str2)

    j = 0    # Index of str1
    i = 0    # Index of str2

    while j<m and i<n:
        if str1[j] == str2[i]:
            j = j+1
        i = i + 1

    # If all characters of str1 matched, then j is equal to m
    return j==m

def abbrv_match(name1, name2):

    n1 = name1.split()
    n2 = name2.split()

    abr_n1 = ""
    abr_n2 = ""

    for word in n1:
        abr_n1 += word[0]

    for word in n2:
        abr_n2 += word[0]

    # print(abr_n1, abr_n2)
    if(isSubSequence(abr_n1, abr_n2) or isSubSequence(abr_n2, abr_n1)):
        return True

    return False

def nameMatch(name1,name2):

    lgth = min(len(name1), len(name2))

    # print("nameMatch", name1, name2)
    if abbrv_match(name1,name2):
        noCommon1 =  " ".join([w for w in name1.split()  if w not in name2.split() ])
        noCommon2 =  " ".join([w for w in name2.split()  if w not in name1.split() ])

        jaro_score = get_jaro_distance(name1, name2)
#         print("Jaro : " , jaro_score)
        editDist_score = editdistance.eval(name1, name2)
#         print("Edit : ", editDist_score)
        sound_score, sond1, sond2 = pheonetic_distance(noCommon1, noCommon2)
#         print("soundness : ", sound_score)
        s_lgth = min(len(sond1), len(sond2))

        if (lgth <= 8  and jaro_score >= 0.94) or (lgth > 8 and  lgth <= 12 and jaro_score >= 0.9) or (lgth > 12 and jaro_score >= 0.87):
#             print("Case 1" )
            return True

        elif (lgth <= 3 and editDist_score < 1) or (lgth > 3 and lgth <= 8 and editDist_score < 2) or (lgth > 8 and editDist_score <= 3):
#             print("Case 2")
            return True

        elif (s_lgth ==0) or (s_lgth <= 3 and sound_score < 1) or (s_lgth > 3 and s_lgth <= 8 and sound_score < 2) or (s_lgth > 8 and sound_score <= 3):
#             print("Case 3")
            return True

    return False


def getBestName(nameList):
    # print("finding best name,", nameList)
    candNames = set()
    for idx,name in enumerate(nameList):

        # remove repeations  eg. akshay gupta akshay gupta => akshay gupta
        nm_list = name.split()
        name = ""
        for id,nm in enumerate(nm_list):
            if nm not in nm_list[:id]:
                name += nm + " "
            else:
                break

        name =  name.strip()
        candNames.add(name)

    candNames = list(candNames)
    bestName = candNames[0]
    for i in range(1,len(candNames)):
        if (len(bestName) >  len(candNames[i])) and (len(candNames[i].split()) > 1):
            bestName =  candNames[i]

    # print("best name,", bestName)

    return bestName


def resolveNames(Names):
    resolvedNames = [Names[0]]

    for i in range(1,len(Names)):
        isMatched = False
        for j in range(len(resolvedNames)):
            try:
                if nameMatch(resolvedNames[j], Names[i]):
                    if (len(resolvedNames[j]) >  len(Names[i])) and (len(Names[i].split()) > 1):
                        resolvedNames[j] =  Names[i]
                        isMatched = True
            except Exception as e:
                print(e)
        if isMatched == False:
            resolvedNames.append(Names[i])

    return resolvedNames


def get_person_names(names):
    distinct_names = set()

    for name in names:
        name = name.lower()
        res = es.search(index=es_index, body=findES_Doc(name))
        # print(name, end = ': ')
        if(len(res['hits']['hits']) != 0):
            candidateNames = [res['hits']['hits'][0]['_source']['stdName']]  +  res['hits']['hits'][0]['_source']['aliases']
            bestName = getBestName(candidateNames)
            distinct_names.add(bestName.lower())

    #resolving names in each articles
    if(len(distinct_names) > 1):
        resolvedNames = resolveNames(list(distinct_names))
        return resolvedNames
    else:
        return []


def extractPersons(collection, collName):
    # 1. sare person ka map [std/aliases] key : value count
    # 2. pq top k

    cursor = collection.find({'entities':{"$exists":True}}).batch_size(50)
    person = dict()     # list of names
    for article in tqdm(cursor):
        name_list = set()
        for per in article['entities']:
            if per['type'] == "Person":
                if 'yojana' not in per['name'].lower() and 'schemes' not in per['name'].lower():
                    name_list.add(per['name'].lower()) # stores all person mentioned in an article
        name_list = get_person_names(name_list)

        for name in name_list:
            if 'yojana' not in per['name'].lower() and 'schemes' not in per['name'].lower():
                if name not in person:
                    person[name]=1
                else:
                    person[name]+=1

    cursor.close()


    res = {key: val for key, val in sorted(person.items(), key = lambda ele: ele[1], reverse = True)}
    return res





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

res = extractPersons(collection, collName)

# with open("./res", 'wb') as f:
#     pickle.dump(res,f)

# res = {}
# with open("./res", 'rb') as f:
#     res = pickle.load(f)

# finally resolving top 50 person entites and plotting top 20 of the resolved ones
Person_names  = list(res.keys())[:50]
for i in tqdm(range(len(Person_names))):
    j = i + 1
    while(j < len(Person_names)):
        print(Person_names[i], Person_names[j])
        if(nameMatch(Person_names[i], Person_names[j])):
            res[Person_names[j]] += res[Person_names[i]]
            del res[Person_names[i]]
            break
        j += 1

res = {key: val for key, val in sorted(res.items(), key = lambda ele: ele[1], reverse = True)}

top_n = 21
per = list(res.keys())[:top_n]
cnt = list(res.values())[:top_n]

total_sum = sum(cnt)
Percentage = [round((value/total_sum) * 100, 2) for value in cnt]
graph = plt.bar(per, cnt)
i = 0
for p in graph:
    width = p.get_width()
    height = p.get_height()
    x, y = p.get_xy()
    plt.text(x+width/2,
             y+height*1.01,
             str(Percentage[i])+'%',
             ha='center',
             weight='bold', rotation = 90)
    i+=1
plt.xticks(rotation = 90)
plt.title(collName)
plt.tight_layout()
plt.savefig('output/' + collName)
plt.clf()
