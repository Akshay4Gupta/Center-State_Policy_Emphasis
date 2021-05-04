from pymongo import MongoClient
import pickle
import matplotlib.pyplot as plt

def load(filename):
    with open(filename, 'rb') as file:
        return pickle.load(file)

def something(collection, collName):
    dt_to_state = load('DTCode_to_State')
    cursor = collection.find({"$or":[{'entities':{"$exists":True}}, {'districtsLocation':{"$exists":True}}]}).batch_size(50)
    states = dict()
    for article in cursor:
        distinct_states = set()
        # for state_entity in article['entities']:
        #     if state_entity['type'] == 'ProvinceOrState':
        #         distinct_states.add(state_entity['name'].upper())


        dist_tuples = article['districtsLocation']
        for dist_tuple in dist_tuples:
            distinct_states.add(dt_to_state[dist_tuple[1]][1])
        for state in distinct_states:
            if state in states:
                states[state] += 1
            else:
                states[state] = 1
    print(states)
    cursor.close()
    res = {key: val for key, val in sorted(states.items(), key = lambda ele: ele[0])}
    st = res.keys()
    cnt = res.values()
    plt.bar(st, cnt)
    plt.xticks(rotation = 90)
    plt.title(collName)
    plt.tight_layout()
    plt.savefig(collName)
    plt.clf()

if __name__ == '__main__':
    client  = MongoClient('mongodb://localhost:27017/')
    db=client['media-db2']
    # collName = input("Enter the collection Name: ")
    collNames = ['development']
    for collName in collNames:
        collName += '_schemes'
        collection=db[collName]
        something(collection, collName)
