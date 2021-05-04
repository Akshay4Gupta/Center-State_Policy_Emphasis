import sys
try:
    import re
    import os
    import pickle
    from pprint import pprint
    import isArticleRelevant
    print("All modules loaded successfully")
except Exception as e:
    print(e)
    sys.exit(0)
# This script is used to identify categories of articles based on final converged *keyword

startpath = os.getcwd()+'/IdentifyUsingKeywords'

# def expandKeywords():
#     global startpath
#     category = dict()
#     for filename in os.listdir(startpath+"/keywords"):
#         with open(startpath+"/keywords/"+filename, 'r') as f:
#             policy  = filename[:-4]
#             category[policy] = []
#             for line in f:
#                 category[policy].append(line.strip().lower())
#
#     for cat in category.keys():
#         category[cat] = process(category[cat])
#
#     with open(startpath+'/expandedKeywords','wb') as f:
#         pickle.dump(category,f)
#
# def expansion(keywordList):
#     if(len(keywordList) <= 1):
#         return keywordList
#     furtherExpandedList = expansion(keywordList[1:])
#     f = [keywordList[0] + ' ' + x for x in furtherExpandedList]
#     f += [keywordList[0] + '-' + x for x in furtherExpandedList]
#     return f
#
# def process(keywords):
#     '''
#     Used to expand the keyword to improve identification
#     '''
#     expandedKeywords = list()
#     for keyword in keywords:
#         expandedKeywords += expansion(keyword.replace('-',' ').strip().split())
#     return expandedKeywords





def identifyCategory(article):

    '''
    takes articles text as input and returns category   of the articles based on keywords
    '''
    global startpath
    res = []
    for filename in os.listdir(startpath+"/keywords"):
        category  = filename[:-4].strip()

        if isArticleRelevant.isArticleRelatedToTopic(article, category):
            res.append(category)

    return res



    # with open(startpath+"/expandedKeywords",'rb') as f:
    #     category  = pickle.load(f)
    #     res = []
    #     for cat in category.keys():
    #         cnt = 0
    #         for keyword in category[cat]:
    #             searched = re.findall(r"\b{}(s|es|'s)?\b".format(keyword), article, re.IGNORECASE)
    #             cnt+=len(searched)
    #             if(cnt >= 3): # threshold
    #                 res.append(cat)
    #                 break
    #         #print(cat + "  " + str(cnt))
    #
    #     return res

if __name__ == '__main__':

    #keywords = ['vivek s ingh', 'akshay       gupta', ' spaceissues']
    #expandedKeywords = process(keywords)
    #expandKeywords()
    #with open(startpath+"/expandedKeywords",'rb') as f:
     #   category = pickle.load(f)
     #   pprint(category)
    #print(expandedKeywords)

    #print(ose.getcwd())
    art = "gst gst gst farmer farmer cashless cashless cashless agriculture agri"
    print(identifyCategory(art))
