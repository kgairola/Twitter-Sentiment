# -*- coding: utf-8 -*-
"""
Created on Tue Mar 28 17:46:58 2017

@author: Kartik Gairola
"""
import re 
import json
import string

# Creating a global varibale : Attribution dictionary : AtrbDict
AtrbDict = {}

class TwitterExtractor:
    '''
    This is a module that helps in extracting the attributes from the twitter file
    
    '''
    def __init__(self, filename, AtrbtExtractL):
        self.filename = filename
        self.AtrbtExtractorL = AtrbtExtractL
        
    def invalidStringRemoval(self,text):
        '''
        Removing all the invalid/ special character. Only aphabets are allowed
        '''
        # removing all the punctions
        regex = re.compile('[%s]' % re.escape(string.punctuation))
        text = regex.sub('', text)
        # removing all the digits
        regex = re.compile('[%s]' % re.escape(string.digits))
        text = regex.sub('', text)
        # removing all the emoticons
        text = text.encode('ascii',errors='ignore').decode("utf-8").strip()
        return text
             
    def dataExtractor(self, new_tweet, index):
        '''
        Extracting the relevant attributes from "new_tweet" and storing it in the glabal dictionary "AtrbDict"
        
        '''        
        global AtrbDict
        temp_attr = {} # Temperary Dictionary
            
        # Creating a nested dictionary to capture all the relevant data ( geo, created_at, phone, location)        
        for attr in self.AtrbtExtractorL:
            if attr == "tweet_id":
                try: 
                    temp_attr["tweet_id"] = new_tweet.get("id_str")
                except:
                    continue # Primary Key , has to be unique and not null
                    
            if attr == "hashtag":
                try:
                    tmp_dict = new_tweet.get("entities").get("hashtags")
                    if len(tmp_dict)>0:
                        hashtagL = []
                        for i in range(len(tmp_dict)):
                            hashtagL.append(tmp_dict[i].get('text'))
                            temp_attr["hashtag"] = ' '.join(hashtagL)
                    else:
                        temp_attr["hashtag"] = ""
                except:
                    temp_attr["hashtag"] = ""
            
            if attr == 'retweet_count':
                try:
                    temp_attr["retweet_count"] = int(new_tweet.get("retweet_count"))
                except:
                    temp_attr["retweet_count"] = -1
                    
            if attr == 'favorite_count':
                try:
                    temp_attr["favorite_count"] = int(new_tweet.get("favorite_count")) 
                except:
                    temp_attr["favorite_count"] = -1
                    
            if attr == "coordinates":
                try:
                    if (new_tweet.get("coordinates") != "null" or new_tweet.get("coordinates") != None):
                        temp_attr["coordinates"]  = new_tweet.get("coordinates")
                    else:
                        temp_attr["coordinates"] = -1
                except:
                    temp_attr["coordinates"] = -1
                    
            if attr == "created_at":
                try:
                    temp_attr["created_at"] = new_tweet.get("created_at")
                except:
                    temp_attr["created_at"] = ""
                    
            if attr == "lang":
                try:
                    temp_attr["lang"] = new_tweet.get("lang")
                except:
                    temp_attr["lang"] = ""         
                            
            if attr == "text_str":
                try:
                    temp_attr["text_str"] = self.invalidStringRemoval(new_tweet.get("text"))
                except:
                    temp_attr["text_str"] = ""
                    
            if attr == "phone":
                try:
                    phone_based = re.findall ( '>(.*?)<', new_tweet.get("source"), re.DOTALL)
                    if str(phone_based[0]).strip().find("iPhone") != -1:
                        temp_attr["phone"] = "iPhone"
                    elif str(phone_based[0]).strip().find("Android") != -1:
                        temp_attr["phone"] = "Android"
                    else:
                        temp_attr["phone"] = "Other"
                except:
                    temp_attr["phone"] = ""
                
            # Capturing all the user data 
            if attr == "u_id":
                try: 
                    temp_attr["u_id"] = new_tweet.get("user").get("id_str")
                except:
                    continue
                   # continue # Primary Key , has to be unique and not null
            
            if attr == "u_screen_name":
                try:
                    temp_attr["u_screen_name"] = new_tweet.get("user").get("screen_name")
                except:
                    temp_attr["u_screen_name"] = ""
                    
            if attr == "u_created_at":
                try:
                    temp_attr["u_created_at"] = new_tweet.get("user").get("created_at")
                except:
                    temp_attr["u_created_at"] = ""
                                
            if attr == "u_lang":
                try:
                    temp_attr["u_lang"] = new_tweet.get("user").get("lang")
                except:
                    temp_attr["u_lang"] = ""
                
            if attr == "u_location":
                try:
                    temp_attr["u_location"] = new_tweet.get("user").get("location")
                except:
                    temp_attr["u_location"] = ""
                
            if attr == "u_followers_count":
                try:
                    temp_attr["u_followers_count"] = int(new_tweet.get("user").get("followers_count"))
                except:
                    temp_attr["u_followers_count"] = -1
                
            if attr == "u_friend_count":
                try:
                    temp_attr["u_friend_count"] = int(new_tweet.get("user").get("friends_count"))
                except:
                    temp_attr["u_friend_count"] = -1
                    
            if attr == "u_status_count":
                try:
                    temp_attr["u_status_count"] = int(new_tweet.get("user").get("statuses_count"))
                except:
                    temp_attr["u_status_count"] = -1
                    
        AtrbDict[index] = temp_attr
    
    def dataRelevantExtract(self):
        ''' 
        From the Data-Dictionary ( Tweet ) , which attributes need to be extracted 
        '''
        global AtrbDict
        index = 0
        with open(self.filename) as Tweet_file:
            for line in Tweet_file:
                try:
                    data = json.loads(line)
                    if data.get("lang") == "en":
                        self.dataExtractor(data, index)
                        index +=1
                except:
                    continue
        return AtrbDict

