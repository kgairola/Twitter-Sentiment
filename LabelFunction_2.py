# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 10:31:37 2017

@author: cesor
"""
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer

#   text = 'Just realized I am at a Starbucks inside a psychotic building.'
    #I knew it smelled psychotic and racist here"""}
               
    #"I NEED Starbucks in my life like ASAP"}

    #"I just wanted mint green tea before class but the line at Starbucks was wild"}
    #"boycott starbucks as their coffee is too sugary and overpriced. They just SUCK"}

#class PkgSentiment:
    

# The Packages Function gets the values from both packages: textblob and vaderSentiment. 
        
def packages(text):
#    text =self.text
    FinalDict={'blob':0,'vader':0}
    blob_val=0
    blob=TextBlob(text)
    blob_val = round(blob.sentiment.polarity,2)
    FinalDict["blob"]=blob_val
    vaderscore=0
    vader = SentimentIntensityAnalyzer()
    vaderscore = vader.polarity_scores(text)
    vaderscore = round(vaderscore["compound"],2)
    FinalDict["vader"]=vaderscore
    return FinalDict

#The Sentiment function takes both packages' values and decides what to label the tweet's sentiment.   
def sentimentAvg(FinalDict):
    sentlist=[]
    for i in (FinalDict.keys()):
        sentlist.append(FinalDict[i])
    average=round(sum(sentlist)/len(sentlist),2)
    return average
    

#sent_output=sentiment(packages(text))


