from stanfordcorenlp import StanfordCoreNLP
import logging
import json
import re, string, os, itertools
import nltk
from nltk.corpus import stopwords
from collections import defaultdict
import os
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from ExtractSentences import ExtractSentences
import top_n_entities
from pymongo import MongoClient
import sys
sys.path.insert(0,"../")
import config

client = MongoClient(config.mongoConfigs['host'],config.mongoConfigs['port'])
db = client[config.mongoConfigs['db']] 
collection=db['aadhar_entities_resolved']                       #collection having resolved entities
art_collection=db['aadhar_articles']                            #collection having articles

N=20                                                            #top N entities
# entity_types = ['Person', 'Company', 'Organization', 'Country', 'City', 'Continent', 'ProvinceOrState']
entity_types = ['Person']
sources_list = ["The Hindu","The Times Of India","Hindustan Times","Indian Express","Deccan Herald","Telegraph", "The New Indian Express"]

extractor = ExtractSentences()             #object for extracting sentences from text
analyzer = SentimentIntensityAnalyzer()

fout = open("bimodal_data.csv",'w')

class StanfordNLP:
    def __init__(self, host='http://localhost', port=9000):
        self.nlp = StanfordCoreNLP(host, port=port,
                                   timeout=30000)
        self.props = {
            'annotators': 'tokenize,ssplit,pos,lemma,ner,parse,depparse,dcoref,relation',
            'pipelineLanguage': 'en',
            'outputFormat': 'json'
        }
    def word_tokenize(self, sentence):
        return self.nlp.word_tokenize(sentence)
    def pos(self, sentence):
        return self.nlp.pos_tag(sentence)
    def ner(self, sentence):
        return self.nlp.ner(sentence)
    def parse(self, sentence):
        return self.nlp.parse(sentence)
    def dependency_parse(self, sentence):
        return self.nlp.dependency_parse(sentence)
    def annotate(self, sentence):
        return json.loads(self.nlp.annotate(sentence, properties=self.props))
    @staticmethod
    def tokens_to_dict(_tokens):
        tokens = defaultdict(dict)
        for token in _tokens:
            tokens[int(token['index'])] = {
                'word': token['word'],
                'lemma': token['lemma'],
                'pos': token['pos'],
                'ner': token['ner']
            }
        return tokens


'''
entitySpecificSentimentAnalysis: 

takes two argument
1. Input File
2. Keywords associated with target

and output two list 
1. Articles on target
2. Articles by target
3. Articles not about target 
'''

fixed_keywords = ['says', 'said', 'asks', 'asked', 'told', 'spoke', 'announced']

def calculateSentiment(sentences_list):	
    sent_compound, sent_pos, sent_neg = 0,0,0
    for sentence in sentences_list:
        sentiment = analyzer.polarity_scores(sentence)
        fout.write(str(sentiment["compound"])+',')
        sent_compound += sentiment["compound"]
        #sent_pos += sentiment["pos"]
        #sent_neg += sentiment["neg"]
    fout.write("\n")
    return sent_compound

def entitySpecificCoverageAnalysis(doc_set, entity_keywords):
    sNLP = StanfordNLP()
    onTargetArticles = []
    byTargetArticles = []
    removedArticles = []
    # print(entity_keywords)
    for text in doc_set:
        pos_text = sNLP.pos(text.lower());
        parse_text = sNLP.dependency_parse(text.lower())
        state1 = False
        state2 = False
        # print(entity_keywords)
        for pt in parse_text:
            # if(pos_text[pt[1] - 1][0]=="headed"):
            #     print(pt,pos_text[pt[1] - 1][0],pos_text[pt[2] - 1][0])
            #     print(text)
            if ((pt[0] == 'nsubj') or (pt[0] == 'nmod') or (pt[0] == 'amod') or (pt[0] == 'dobj')) and ((pos_text[pt[1] - 1][0] in entity_keywords) or (pos_text[pt[2] - 1][0] in entity_keywords)):
                if ((pt[0] == 'nsubj') and (pos_text[pt[1] - 1][0] in fixed_keywords or pos_text[pt[2] - 1][0] in fixed_keywords)):
                    state2 = True
                else:
                    state1 = True
        if state1:
            onTargetArticles.append(text)
        if state2:
            byTargetArticles.append(text)
        else:
            removedArticles.append(text)
    return (onTargetArticles, byTargetArticles, removedArticles)

if __name__=='__main__':
    
    fwrite = open('entity_coverage_results.csv','w')
    fwrite_temp = open('aliases_temp.csv', 'w')
    f = open('sanity_check1.txt', 'w')
    
    entities = top_n_entities.get_top_n_entities(collection, N, entity_types)
    
    about_entity_coverages = {s:defaultdict(int) for s in sources_list}
    by_entity_coverages = {s:defaultdict(int) for s in sources_list}

    fwrite.write('            ')
    for source in sources_list:
        fwrite.write(source + ' ')
    fwrite.write('\n')
    
    aliases = []
    modified_aliases = []
    for type in entities.keys():
        for entity in entities[type]:
            temp_aliases = []
            for alias in entity['aliases']:
                temp_aliases.append('_'.join(alias.split()).lower())
            fwrite_temp.write(','.join(temp_aliases))
            fwrite_temp.write('\n')
            aliases.append(list(map(str.lower,entity['aliases'])))
            modified_aliases.append(temp_aliases)
            
    fwrite_temp.close()
    
    print("Aliases written to \'aliases_temp.csv\'. Please modify the aliases, leave empty row in case of no alias of an entity.")
    print("Press Enter when done...")
    input()
    fwrite_temp = open('aliases_temp.csv', 'r')
    aliases = [list(map(str.lower,map(str.strip,line.split(",")))) for line in fwrite_temp]
    modified_aliases = aliases
    print(modified_aliases)

    os.remove("aliases_temp.csv")
    
    for type in entities.keys():
        for i,entity in enumerate(entities[type]):
            fout.write(entity['name']+'\n')
            # print(entity["name"], aliases[i])
            if(len(modified_aliases[i]) == 1 and modified_aliases[0]==""):
                continue
            fwrite.write(entity['name'] + ',')
            articles = {s:[] for s in sources_list}
            for article_id in entity["articleIds"]:
                article = art_collection.find({"_id":article_id})[0]
                source = article["sourceName"]
                text = article["text"]
                articles[source].append(text)
            for source in sources_list:
                sentences = []
                for art in articles[source]:
                    sentences += extractor.split_into_sentences(art.lower())
                for (a1,a2) in zip(modified_aliases[i],aliases[i]):
                    sentences = list(map(lambda x: re.sub(a2,a1,x),sentences))
                # print("reached")
                # print('\n'.join(sentences[0:1]))
                coverages = entitySpecificCoverageAnalysis(list(sentences), modified_aliases[i])[:-1]
                # print(coverages)
                senti_about = calculateSentiment(coverages[0])
                senti_by = calculateSentiment(coverages[1])
                about_entity_coverages[source][entity['name']] += len(coverages[0])
                by_entity_coverages[source][entity['name']] += len(coverages[1])
                fwrite.write(str(about_entity_coverages[source][entity['name']]) + ',')
                fwrite.write(str(senti_about) + ',')
                #fwrite.write(str(senti_about[1]) + ',')
                #fwrite.write(str(senti_about[2]) + ',')
                fwrite.write(str(by_entity_coverages[source][entity['name']]) + ',')
                fwrite.write(str(senti_by) + ',')
                f.write(entity['name']+'\n\n\n')
                f.write('\n'.join(coverages[0]))
                #fwrite.write(str(senti_by[1]) + ',')
                #fwrite.write(str(senti_by[2]) + ',')
                print(entity['name'],source,"done...")
            fwrite.write('\n')
            print(entity['name'],"done...")
    
    fwrite.close()
    fout.close()
    f.close()

'''
text = "note ban a foolish decision, paytm means pay to modi, says rahul gandhi economictimes"
pos_text = sNLP.pos(text);
parse_text = sNLP.dependency_parse(text)
for i in range(1,len(parse_text)):
  print(parse_text[i][0] + ' ' + pos_text[parse_text[i][1]-1][0] + ' ' + pos_text[parse_text[i][2]-1][0] )
'''
