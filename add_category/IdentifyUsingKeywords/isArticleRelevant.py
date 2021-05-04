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


def readfromFile(filename):
    aliases = []
    with open('IdentifyUsingKeywords/keywords/' + filename + '.txt', 'r+') as file:
        result = ''
        for line in file:
            if len(line) > 1:
                aliases.append(lemmatize_sentence(line.strip()))
    return aliases

def isArticleRelatedToTopic(article, keyword):
    # step0: split into sentences
    # print(article)
    aliases = readfromFile(keyword)
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
    return False
