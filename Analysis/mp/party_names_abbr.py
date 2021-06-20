import pickle
from pprint import pprint
with open("./coalition" ,'rb') as f:
    coalition = pickle.load(f)
pprint(coalition)

for alliance in coalition :
    parties = coalition[alliance]
    new_parties = parties.copy()
    for party in parties:
        names = party.split()
        abbr = ""
        if len(names) > 1:
            for name in names:
                if name == "and" or name == 'of' or name =="&":
                    continue
                if name[0] =='(':
                    abbr += "(" + name[1] + ")"
                else :
                    abbr += name[0]
            abbr = abbr.lower().strip()
            new_parties.append(abbr)
    coalition[alliance] = new_parties

with open("./coalition", 'wb') as f:
    pickle.dump(coalition, f)
