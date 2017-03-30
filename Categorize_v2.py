# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import json
import re
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords


# Create function to load menus from text file
def loadCategoryWords (filename, dWords, key):
    
    # Number of arguments in list filename and key should be the same
    if len(filename) != len(key):
        print ('Input error')
        return -1

    # Start loading keyword
    else:
        for fn, k in zip(filename, key):          
            f = open("./Categories/" + fn)
            for line in f.readlines():
                # Remove punctuation
                regex = re.compile('[%s]' % re.escape(string.punctuation))
                line = regex.sub('', line)
                # Make it as a list of words
                item = line.strip('\n').split()
                # Put words in dictionary
                for word in item:
                    if k not in dWords:
                        dWords[k] = [word]
                    else:
                        dWords[k].append(word)
            f.close()
            
            # Make it lower case
            dWords[k] = {token.lower() for token in dWords[k] }        
            # Make value as a set
            dWords[k] = set(dWords[k])   
            # Remove stopwords
            dWords[k] = dWords[k] - set(stopwords.words('english'))   
            # Set of unuseful word in menus
            unuseful_word = {'add', 'fe', '&', 'starbucks', 'very', 
                             'cool'}   
            # Remove Unuseful word
            dWords[k] = set(dWords[k]) - unuseful_word
            
        return dWords


# Fuction to categorize tweet whether this tweet is food, service, ambience related
def categorizeTweet (tweet, dWords):
    # Initialize categories which will be a return of this function
    categories = [0, 0, 0, 0] # Sequence ambiance menu service general
    ### Preprocess tweet
    # Remove punctuation
    regex = re.compile('[%s]' % re.escape(string.punctuation))
    tweet = regex.sub('', tweet)
    # Make it lowercase
    tweet = tweet.split()
    tweet = set([word.lower() for word in tweet])

    for key in dWords.keys():
        if tweet & dWords[key] != set():            
            if key == "ambiance":
                categories[0] = 1
            elif key == "food":
                categories[1] = 1
            elif key == "service":
                categories[2] = 1
    # If it doesn't contain 3 categories, mark it as general
    if sum(categories) == 0:
        categories[3] = 1
                       
    return categories
        






















