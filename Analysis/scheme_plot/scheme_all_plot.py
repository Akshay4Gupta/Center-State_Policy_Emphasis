from pymongo import MongoClient
import pandas as pd
import pickle
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pickle
import re
from dateutil import parser
from datetime import datetime
from collections import namedtuple
import matplotlib.pyplot as plt
import numpy as np

def min_max(v):
    return (v - v.min()) / (v.max() - v.min())

client  = MongoClient('mongodb://localhost:27017/')
db=client['media-db2']


def get_article_count(collName, startDate, endDate):
    collection=db[collName]
    return  collection.count_documents({'$and':[{'publishedDate':{'$gte':startDate}},{'publishedDate':{'$lte':endDate}}]})


# print(get_article_count('agriculture_schemes', '2019-05-05', '2020-05-05'))

df = pd.read_excel('top5_perCapita.xlsx', sheet_name = ['Population', 'humanDevelopment', 'health_hygiene', 'agriculture'])

with open("coalition", 'rb') as f:
    coalition  = pickle.load(f)

density  = {}
for den, sts in zip(df['Population']['total']/1000000,df['Population']['State']):
    density[sts] = den



# party  Alliance during UPA in center
nda_2009 = coalition['nda_2009']
upa_2009 = coalition['upa_2009']

# party  Alliance during NDA in center
nda_2014_19 = coalition['nda_2014'] + coalition['nda_2019']
upa_2014_19 = coalition['upa_2014'] + coalition['upa_2019']


large_states = ['andhra pradesh',
  'kerala',
  'maharashtra',
  'odisha',
  'rajasthan',
  'tamil nadu',
  'uttar pradesh',
  'madhya pradesh',
  'nct of delhi']

medium_states = ['assam',
  'bihar',
  'chhattisgarh',
  'gujarat',
  'haryana',
  'himachal pradesh',
  'jammu & kashmir',
  'jharkhand',
  'karnataka',
  'punjab',
  'west bengal']

small_states = ['arunachal pradesh',
  'goa',
  'manipur',
  'meghalaya',
  'mizoram',
  'nagaland',
  'puducherry',
  'sikkim',
  'tripura',
  'uttarakhand']

# state_category = {'small':small_states, "medium":medium_states, "large":large_states}

states_all  = small_states + medium_states + large_states
state_category = {'all':states_all}

with open('state', 'rb') as f:
    data = pickle.load(f)


schemes = ["agriculture", "health_hygiene", "humanDevelopment"]
# schemes = ["agriculture"]
test_count = {}


for  cat in state_category.keys():
    all_states = state_category[cat]

    net_aligned, net_non_aligned = 0,0
    for scheme in schemes:

#         x = all_states
        UPA_UPA = [0]*len(all_states)
        UPA_NDA = [0]*len(all_states)
        UPA_NON = [0]*len(all_states)


        NDA_NDA= [0]*len(all_states)
        NDA_UPA = [0]*len(all_states)
        NDA_NON = [0]*len(all_states)


        for st_idx, state in enumerate(all_states):
            center_state = list(data[scheme][state].keys())

            UPA_UPA_total_count, UPA_NDA_total_count, UPA_NON_total_count = 0,0,0
            NDA_NDA_total_count, NDA_UPA_total_count, NDA_NON_total_count = 0,0,0
            for c_s in center_state:

                c, s = c_s.split(" : ")
                if c == "INC":
                    if s.lower().strip() in upa_2009:
                        UPA_UPA[st_idx] +=  data[scheme][state][c_s][0][0] #count
                        UPA_UPA_total_count += get_article_count(scheme+"_schemes", data[scheme][state][c_s][1][0],  data[scheme][state][c_s][1][1] )

                    elif s.lower().strip() in nda_2009:
                        UPA_NDA[st_idx] +=  data[scheme][state][c_s][0][0] #count
                        UPA_NDA_total_count += get_article_count(scheme+"_schemes", data[scheme][state][c_s][1][0],  data[scheme][state][c_s][1][1] )

                    else:
                        UPA_NON[st_idx] +=  data[scheme][state][c_s][0][0] #count
                        UPA_NON_total_count += get_article_count(scheme+"_schemes", data[scheme][state][c_s][1][0],  data[scheme][state][c_s][1][1] )

                else:
                    if s.lower().strip() in upa_2014_19:
                        NDA_UPA[st_idx] +=  data[scheme][state][c_s][0][0] #count
                        NDA_UPA_total_count += get_article_count(scheme+"_schemes", data[scheme][state][c_s][1][0],  data[scheme][state][c_s][1][1] )

                    elif s.lower().strip() in nda_2014_19:
                        NDA_NDA[st_idx] +=  data[scheme][state][c_s][0][0] #count
                        NDA_NDA_total_count += get_article_count(scheme+"_schemes", data[scheme][state][c_s][1][0],  data[scheme][state][c_s][1][1] )


                    else:
                        NDA_NON[st_idx] +=  data[scheme][state][c_s][0][0] #count
                        NDA_NON_total_count += get_article_count(scheme+"_schemes", data[scheme][state][c_s][1][0],  data[scheme][state][c_s][1][1] )


            # Per-capita
            NDA_NDA[st_idx] = (NDA_NDA[st_idx]/NDA_NDA_total_count) if NDA_NDA_total_count!=0 else 0
            NDA_UPA[st_idx] = (NDA_UPA[st_idx]/NDA_UPA_total_count) if NDA_UPA_total_count!=0 else 0
            NDA_NON[st_idx] = (NDA_NON[st_idx]/NDA_NON_total_count) if NDA_NON_total_count!=0 else 0

            UPA_UPA[st_idx] = (UPA_UPA[st_idx]/UPA_UPA_total_count) if UPA_UPA_total_count!=0 else 0
            UPA_NDA[st_idx] = (UPA_NDA[st_idx]/UPA_NDA_total_count) if UPA_NDA_total_count!=0 else 0
            UPA_NON[st_idx] = (UPA_NON[st_idx]/UPA_NON_total_count) if UPA_NON_total_count!=0 else 0



        # creating a list of index names
        index_values = np.array(all_states)

        # creating a list of column names
        column_values = ['NDA', 'UPA', 'ThirdFront']

        # creating the dataframe

        UPA_center= np.stack([
                             np.array(UPA_NDA),\
                             np.array(UPA_UPA), \
                             np.array(UPA_NON)
                            ]).T

        NDA_center= np.stack([
                             np.array(NDA_NDA),\
                             np.array(NDA_UPA), \
                             np.array(NDA_NON)
                            ]).T
        df_UPA = pd.DataFrame(data = UPA_center,
                          index = index_values,
                          columns = column_values)

        df_NDA = pd.DataFrame(data = NDA_center,
                          index = index_values,
                          columns = column_values)

        with open(scheme+"UPA", 'wb') as f:
            pickle.dump(df_UPA, f)
        with open(scheme+"NDA", 'wb') as f:
            pickle.dump(df_NDA, f)

        barWidth = 0.25

        fig, (ax1, ax2) = plt.subplots(2, figsize =(15, 12))
        fig.suptitle('Normalized article ratio for {} states with thier coalition with Center : {} scheme'.format(cat, scheme))

        # Set position of bar on X axis
        UPA1 = np.arange(len(all_states))
        UPA2 = [x + barWidth for x in UPA1]
        UPA3 = [x + barWidth for x in UPA2]

        NDA1 = np.arange(len(all_states))
        NDA2 = [x + barWidth for x in NDA1]
        NDA3 = [x + barWidth for x in NDA2]

        NDA_NDA = min_max(np.array(NDA_NDA))
        NDA_UPA = min_max(np.array(NDA_UPA))
        NDA_NON = min_max(np.array(NDA_NON))

        UPA_NDA = min_max(np.array(UPA_NDA))
        UPA_UPA = min_max(np.array(UPA_UPA))
        UPA_NON = min_max(np.array(UPA_NON))

        
        avg_UPA = 0
        for i in range(len(all_states)):
            cnt, total = 0,0
            if UPA_UPA[i] != 0:
                total += UPA_UPA[i]
                cnt +=1
            if UPA_NDA[i] != 0:
                total += UPA_NDA[i]
                cnt +=1
            if UPA_NON[i] != 0:
                total += UPA_NON[i]
                cnt +=1
            if cnt == 0:
                print(all_states[i])
            avg_UPA += total / cnt

        avg_NDA = 0
        for i in range(len(all_states)):
            cnt, total = 0,0
            if NDA_NDA[i] != 0:
                total += NDA_NDA[i]
                cnt +=1
            if NDA_UPA[i] != 0:
                total += NDA_UPA[i]
                cnt +=1
            if NDA_NON[i] != 0:
                total += NDA_NON[i]
                cnt +=1
            if cnt == 0:
                print(all_states[i])
            avg_NDA += total / cnt

        avg_UPA /= len(all_states)
        avg_NDA /= len(all_states)


        R1 = ax1.bar(UPA1, UPA_UPA, color ='b', width = barWidth,
                edgecolor ='grey', label ='UPA')

        R2 = ax1.bar(UPA2, UPA_NDA, color ='orange', width = barWidth,
                edgecolor ='grey', label ='NDA')

        R3 = ax1.bar(UPA3, UPA_NON, color ='g', width = barWidth,
                edgecolor ='grey', label ='Third Front')

        ax1.axhline(y=avg_UPA, color='r', linestyle='-', label="average")
        #**************************************************************

        r1 = ax2.bar(NDA1, NDA_UPA, color ='b', width = barWidth,
                edgecolor ='grey', label ='UPA')

        r2 = ax2.bar(NDA2, NDA_NDA, color ='orange', width = barWidth,
                edgecolor ='grey', label =' NDA')

        r3 = ax2.bar(NDA3, NDA_NON, color ='g', width = barWidth,
                edgecolor ='grey', label ='Third Front')
        ax2.axhline(y=avg_NDA, color='r', linestyle='-', label="average")

        ax1.set_xticks([r + barWidth for r in range(len(all_states))])
        ax1.set_xticklabels(all_states, fontsize=12,  rotation = 90)
        ax1.set_ylabel('UPA: Normalized article ratio')
        ax1.set_xlabel(cat)

        ax2.set_xticks([r + barWidth for r in range(len(all_states))])
        ax2.set_xticklabels(all_states, fontsize=12, rotation = 90)
        ax2.set_ylabel('NDA: Normalized article ratio')
        ax1.set_xlabel(cat)


        ax1.legend()
        ax2.legend()
        plt.tight_layout()
        fig.savefig(cat + "__" + scheme )
