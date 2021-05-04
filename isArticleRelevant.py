import sys
try:
    import time
    import json, nltk, math, os
    import numpy as np
    from ExtractSentences import ExtractSentences
    from pymongo import MongoClient
    from nltk.tokenize import word_tokenize, sent_tokenize
    from stanfordcorenlp import StanfordCoreNLP
    import nltk
    from nltk.stem import WordNetLemmatizer
    from nltk.corpus import wordnet
    from nltk.tag.stanford import StanfordPOSTagger
    print("All modules loaded successfully")
except Exception as e:
    print(e)
    sys.exit(0)


lemmatizer = WordNetLemmatizer()

# function to convert nltk tag to wordnet tag
def nltk_tag_to_wordnet_tag(nltk_tag):
    if nltk_tag.startswith('J'):
        return wordnet.ADJ
    elif nltk_tag.startswith('V'):
        return wordnet.VERB
    elif nltk_tag.startswith('N'):
        return wordnet.NOUN
    elif nltk_tag.startswith('R'):
        return wordnet.ADV
    else:
        return None

def lemmatize_sentence(sentence):
    #tokenize the sentence and find the POS tag for each token
    nltk_tagged = nltk.pos_tag(nltk.word_tokenize(sentence))
    #tuple of (token, wordnet_tag)
    wordnet_tagged = map(lambda x: (x[0], nltk_tag_to_wordnet_tag(x[1])), nltk_tagged)
    lemmatized_sentence = []
    for word, tag in wordnet_tagged:
        if tag is None:
            #if there is no available tag, append the token as is
            lemmatized_sentence.append(word)
        else:
            #else use the tag to lemmatize the token
            lemmatized_sentence.append(lemmatizer.lemmatize(word, tag))
    return " ".join(lemmatized_sentence)


aliases = []
def readfromFile(filename):
    with open('themes/' + filename + '.txt', 'r+') as file:
        result = ''
        for line in file:
            if len(line) > 1:
                aliases.append(lemmatize_sentence(line.strip()))

print("Enter Theme Name : ")
theme = input()
coll_name = theme.lower()
readfromFile(coll_name)

print(aliases)

client = MongoClient('mongodb://127.0.0.1:27017/')
my_db = client['media-db2']
coll = my_db[coll_name]


# 'Enter the fixword that u want aliases to be replaced with '
fix_keyword = coll_name

#coreNLP setup
# nlp = StanfordCoreNLP(r'/home/vivek/Data/IITDcourse/Sem4/MTP2/findArticles/stanford-corenlp-full-2018-10-05')





def isArticleRelatedToTopic(article, aliases, keyword):
    # step0: split into sentences
    # print(article)
    article = lemmatize_sentence(article)
    # print(article)
    extractor = ExtractSentences()
    sent_text = np.array(extractor.split_into_sentences(article))

    # step1 lower the text
    text = article.lower()
    keyword = keyword.lower()

    # step2 replace aliases from keyword
    for a in aliases:
        text = text.replace(str(' ') + a.lower() + str(' '), str(' ') + keyword + str(' '))

    # accept article that have keyword's frequency greater than freq_threshold
    freq_threshold = 2
    key_freq = 0
    for word in nltk.word_tokenize(text):
        if word == keyword:
            key_freq = key_freq + 1

    if key_freq > freq_threshold:
       # print('\tKeyword frequency ', key_freq)
        return True

    # accept the articles where keyword is in top line_threshold lines
    occ_threshold = 0.5
    sent_text = np.atleast_1d(sent_text)
    top_sent = sent_text[:int(math.ceil(occ_threshold * len(sent_text)))]
    for ts in top_sent:
        ts = ts.lower()
        ts = ts.replace('.', ' ').replace(',', ' ').replace('-',' ')
        for a in aliases:
            ts = ts.replace(str(' ') + a.lower() + str(' '), str(' ') + keyword + str(' '))
        if keyword in nltk.word_tokenize(ts):
           # print('\t top 50% lines')
            return True

    # **************************************************************************************************
    # # accept if keyword is present in any of selected relations
    # try:
    #     # print("text: ", text, type(text))
    #     pos_text = nlp.pos_tag(text)
    #     # print("pos_text: ", pos_text)
    #     parse_text = nlp.dependency_parse(text)
    #     # print("parse_text: ", parse_text)
    #     selected_relation = ['amod', 'nmod', 'dobj', 'iobj', 'nsubj', 'nsubjpass']
    #
    #     for i in range(1, len(parse_text)):
    #         rel = parse_text[i][0]
    #         word1 = pos_text[parse_text[i][1] - 1][0]
    #         word2 = pos_text[parse_text[i][2] - 1][0]
    #         if (word1 == keyword or word2 == keyword) and (rel in selected_relation):
    #           #  print('\t passed NLP')
    #             return True
    # except Exception as e:
    #     print('Error from stanfordcorenlp check : ', e)
    # **************************************************************************************************


    return False

print('Fetch the related article text..')
cursor = coll.find({}, no_cursor_timeout = True).batch_size(50)

print("Removing irrelevant articles")

startTime = time.time()

count = 0
rem = 0
rem_id = []
total_deleted = 0
for art in cursor:
    text = art['text']
    count = count + 1
    if count % 5000 == 0:
        print(count, ": total_count")
    if not isArticleRelatedToTopic(text, aliases, fix_keyword):
        rem_id.append(art['_id'])
        # res  = coll.delete_one({'_id': art['_id']})
        # rem += 1
        if len(rem_id) > 3000:
            total_deleted += len(rem_id)
            res = coll.delete_many({'_id': { '$in': rem_id}})
            print("total_deleted : ", total_deleted)
            rem_id = []




if len(rem_id) > 0:
    total_deleted += len(rem_id)
    res = coll.delete_many({'_id': { '$in': rem_id}})

print("total_deleted : ", total_deleted)
