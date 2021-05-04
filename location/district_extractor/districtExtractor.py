# vk ak
import pickle
from pprint import pprint
import re
import os

def loc_namesMAP():
    with open("district_extractor/loc_name_DT_code_DICT", 'rb') as f:
    #with open("/home/gem2/media_filter/media_monitor/opencalais/district_extractor/loc_name_DT_code_DICT", "rb") as f:
        loc_names = pickle.load(f)
    return loc_names

#official names of states wrt DTcodes
def offical_names():
    #with open("/home/gem2/media_filter/media_monitor/opencalais/district_extractor/DT_code_official_name", "rb") as f:
    with open("district_extractor/DT_code_official_name",'rb') as f:
        offical_names = pickle.load(f)
    return offical_names

# to remove subset district
# eg. if gautam budh nagar is present then all proper subset of 'gautam budh nagar' must be removed.
# otherwise nagar which is also a district name will be added although article is not actually taking about it.
#eg. remove "nagar" keep "gautam budh nagar"
def remove_subset_name(dictionary):
    li  = set()
    for key1 in dictionary:
        flag = 0
        for key2 in dictionary:
            if key1!=key2 and re.search(key1,key2):
                flag =1 # loc is subset
        if flag ==0:
            li.add(dictionary[key1])
    return li

def districtFinder(articleText):
    loc_names = loc_namesMAP()
    official_name = offical_names()
    articleWords = articleText.replace("."," ").replace(',', ' ').lower().split()
    loc_dict = dict()
    l =[]
    #crude way for location names of 5 words
    for i in range(1, 6):
        for j in range(len(articleWords)):
            dtName = ' '.join(articleWords[j:j+i])
            # print(dtName)
            # 700 and above for state names
            if  dtName in loc_names and loc_names[dtName] < 700:
                loc_dict[dtName] = loc_names[dtName]

    dtCodeList= list((remove_subset_name(loc_dict)))
    finallist = []
    for i in dtCodeList:
        finallist.append((official_name[i], i))
    return finallist
# This is just an example to demonstrate how the district is extracted and returned 
if __name__ == '__main__':
    text = '''Several teams of policemen attached to Prohibition Enforcement Wing (PEW) raided the hideouts of illegal hooch makers, destroyed materials used for manufacturing country liquor and arrested 11 persons in this connection.A total of 27 cases were registered in three districts of Vellore, Tirupattur and Ranipet on Thursday.Five DSPs attached to PEW, three DSPs attached to Law and Order, 23 Inspectors, 24 Sub Inspectors, 235 policemen (PEW 175, Armed Reserve Force 20, Special Task Force 40) were involved in these raids and the raids are still on.More than 3,300 litres of fermented wash and 710 litres of illicitly brewed liquor were destroyed by the police personnel during the raids in Eripattarai Hills, Goripallam, and Thirukuvapalayam areas. One three wheeler and a two wheeler used for transporting materials were seized by the police. In Ranipet district, 276 liquor bottles were seized by the police, besides a three wheeler and two bikes from the spot. In Tirupattur district, the police team destroyed 2,500 litres of fermented wash and 385 litres of liquor. The police team seized 33 liquor bottles during the raid. In Vellore district, the team destroyed 800 litres of fermented wash and 320 litres of liquor. A total of 33 liquor bottles were seized during the raid.The police have warned the violators of stringent action and publicised contact numbers of police personnel for the public to contact in case if they found anyone indulging in nefarious activities. People can either call, sms or whatsapp the information to the concerned official of their areas. The contact numbers are: Prohibition Enforcement Wing (PEW) DSP: 63741 11389; Superintendent of Police - Vellore: 94427 61681; Superintendent of Police - Tirupattur: 94981 44441; Superintendent of Police-Ranipet: 98429 24016.'''

    loc=districtFinder(text)
    print(loc)
