from pymongo import MongoClient
from tqdm import tqdm
from pprint import pprint
import matplotlib.pyplot as plt
import pickle

client = MongoClient('localhost', 27017)
db = client['media-db2']

with open('state_popluation_perMillion', 'rb') as f:
    population = pickle.load(f)

collNames = ['humanDevelopment','health_hygiene', 'agriculture']
# collNames = ['environment']
for collName in collNames:
    print(collName)
    collection = db[collName+'_schemes']
    cursor = collection.find({'states':{"$exists":True}}).batch_size(50)
    states = dict()
    for article in tqdm(cursor):

        distinct_states = article['states']
        for state in distinct_states:
            if state in states:
                states[state] += 1
            else:
                states[state] = 1



    Res = {}
    for state_name in population:
        if state_name in states:
            Res[state_name] = states[state_name] / population[state_name]

    res = {key: val for key, val in sorted(Res.items(), key = lambda ele: ele[0])}

    total_sum = sum(res.values())
    Percentage = [round((value/total_sum) * 100, 2) for value in res.values()]
    st = res.keys()
    cnt = res.values()
    graph = plt.bar(st, cnt)
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
    plt.title('Net article coverage across states for {}'.format( collName))
    plt.xlabel("states")
    plt.ylabel("Article counts")
    plt.tight_layout()
    plt.savefig('output_loc/' + collName)
    plt.clf()
