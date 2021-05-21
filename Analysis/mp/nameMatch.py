from pyjarowinkler.distance import get_jaro_distance
import editdistance
import fuzzy
from tqdm import tqdm


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
