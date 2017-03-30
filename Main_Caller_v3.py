# -*- coding: utf-8 -*-
"""
Created on Fri Mar 28 17:05:01 2017

@author: Kartik Gairola
"""

import Attribute_Extractor
import Twitter_Db
import progressbar
from time import sleep
import pandas as pd
import re 
import string
from nltk.corpus import stopwords
from stemming.porter2 import stem
import LabelFunction_2
import Categorize_v2


def getCred(filename):
    '''
    Returns a dictionary with all the important user details
    '''
    credDict = {}
    cred_file = open(filename, "r")
    cred_file.readline()
    temp_str=""
    for line in cred_file:
        temp_str = temp_str + line.strip()
        tempL = temp_str.strip().replace("{","").replace("}","").replace('"','').split(",")
    for i in range(len(tempL)):
        comb = tempL[i]
        credDict[comb.split(":")[0]] = comb.split(":")[1]
    return credDict

def TextPreprocess(text, punc = "yes", digit = "yes", char = "yes", stop_word = "yes", stem_word = "yes", unique_word = "no" ):
    '''
    Pre-Processing the data
    '''    
    text = text.lower()
    text = ''.join([word if ord(word) < 128 else ' ' for word in text])   
    if punc == "yes":
        # remove all punctuation
        regex = re.compile('[%s]' % re.escape(string.punctuation))
        text = regex.sub('', text)   
    if digit == "yes":
        # remove all digits
        regex = re.compile('[%s]' % re.escape(string.digits))
        text = regex.sub('', text)
    if char == "yes":
        # only retain words >= 4 characters long
        text = ' '.join([word for word in text.split() if (len(word)>=4)])
    if stop_word == "yes":
        # remove stop words
        sw = stopwords.words("english")
        text = ' '.join([word for word in text.split() if word not in sw])    
    if stem_word == "yes":
        # convert to stem words
        text = ' '.join([stem(word) for word in text.split()])
    if unique_word == "yes":
        # only retain unique words
        text = ' '.join(set(text.split()))
    
    return text

def PreProcTuple_To_Dataframe(data):
    ''' 
    Converts the data extracted from MYSQL into a dataframe for pre-processing
    '''
    tweet_idL, textL = [] , []
    for i in range(len(data)):
        tweet_idL.append(data[i][0])
        textL.append(data[i][1])
    df = pd.DataFrame({'tweet_id': tweet_idL, 'text': textL})
    del tweet_idL, textL
    return df

def SentCatTuple_To_Dataframe(data):
    '''
    Computes the sentiment and the category of the tweets from MYSQL into a dataframe 
    '''
    tweet_idL, textL, pkgvalL, ambianceL, menuL, serviceL, generalL = [], [], [], [], [], [], []
    for i in range(len(data)):
        tweet_idL.append(data[i][0])
        textL.append(data[i][1])
        pkgvalL.append(data[i][2])
        ambianceL.append(data[i][3])
        menuL.append(data[i][4])
        serviceL.append(data[i][5])
        generalL.append(data[i][6])
        df = pd.DataFrame({'tweet_id': tweet_idL, 'text': textL, 'pkgval': pkgvalL, 'ambiance':ambianceL, 
                           'menu':menuL, 'service':serviceL, 'general':generalL})
    del tweet_idL, textL, pkgvalL, ambianceL, menuL, serviceL, generalL
    return df

def main(filename):

    credDict = getCred(filename)
    # global tweet data file
    tweet_data_file = credDict["tweet_file"]
    extractL=["tweet_id", "coordinates", "created_at", "lang", "text_str", "phone", 'hashtag', 'retweet_count', 'favorite_count',
        'u_id', 'u_screen_name', 'u_created_at', 'u_lang', 'u_location', 'u_followers_count', 'u_friend_count','u_status_count']
    
    # Extracting Data from the twitter file and creating a dictionary
    ObjAtrbextractor = Attribute_Extractor.TwitterExtractor(tweet_data_file, extractL)
    new_attrD = ObjAtrbextractor.dataRelevantExtract()
    print("Attribute Extraction Complete")

# -----------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------

    print("Populating data into Raw_Tweets/UserDetails")    
    # Populating the data in the database
    insert_counter = 0
    update_counter = 0
    fail_counter = 0
    print("Total Records : ", len(new_attrD))
    objdb = Twitter_Db.Database_Interaction(credDict["db_user"],credDict["db_pass"])
    ver = objdb.CheckConnection()
    if (str(ver[0])) != "null":
        # Checking for the slot_id
        try:
            sql_query = '''select max(slot_id) from %s''' % ( credDict["db_name"] +'.'+credDict["tbl_name2"] )
            slot_id = int(objdb.ExtractOneQuery(sql_query)[0])
            slot_id +=1
        except:
            slot_id = 0
        bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)        
        sleep(1)
        for i in range(len(new_attrD)):
            try:
                sql_query = '''INSERT INTO %s(u_id, u_screen_name, u_created_at, u_lang, u_location, u_followers_count, u_friend_count, u_status_count)
                VALUES ("%s", "%s", "%s", "%s", "%s", %d, %d, %d)''' % ( credDict["db_name"] +'.'+credDict["tbl_name1"], 
                new_attrD[i].get("u_id"), new_attrD[i].get("u_screen_name"), new_attrD[i].get("u_created_at"), new_attrD[i].get("u_lang"),
                new_attrD[i].get("u_location"), new_attrD[i].get("u_followers_count"), new_attrD[i].get("u_friend_count") ,
                new_attrD[i].get("u_status_count"))  
                objdb.InsertQuery_new(sql_query)
                insert_counter +=1
            except:
                sql_query = '''UPDATE %s
                set u_followers_count = %d, u_friend_count = %d, u_status_count = %d;
                where u_id = "%s"''' % ( credDict["db_name"] +'.'+credDict["tbl_name1"], 
                new_attrD[i].get("u_followers_count"), new_attrD[i].get("u_friend_count"), new_attrD[i].get("u_status_count"), new_attrD[i].get("u_id"))  
                objdb.InsertQuery_new(sql_query)
                update_counter +=1 
            try:
                sql_query = '''INSERT INTO %s(brand, hashtag, retweeted_count, favourite_count, geo_coordinate, created_at, lang, text_str, phone, u_id, tweet_id, slot_id)
                VALUES ("%s", "%s", %d, %d, "%s", "%s", "%s", "%s", "%s", "%s", "%s", %d )''' % ( credDict["db_name"] +'.'+credDict["tbl_name2"],
                credDict["brand"], new_attrD[i].get("hashtag"), new_attrD[i].get("retweet_count"), new_attrD[i].get("favorite_count"),
                new_attrD[i].get("coordinates"), new_attrD[i].get("created_at"), new_attrD[i].get("lang"), new_attrD[i].get("text_str") , 
                new_attrD[i].get("phone"), new_attrD[i].get("u_id"), new_attrD[i].get("tweet_id"), slot_id)   
                objdb.InsertQuery_new(sql_query)
            except:
                fail_counter +=1
            bar.update(i)
        print()
        print("Total User Records : ", insert_counter)
        print("Total Updates of User Records : ", update_counter)
        print("Total Rollbacks of Tweet Records : ", fail_counter)

# -----------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------
    
    # Now inserting this new batch into preprocessed_data table using the slot_id
    try:
        sql_query = '''INSERT INTO %s(tweet_id, pre_text_str, slot_id) 
        Select tweet_id, text_str, slot_id from %s where slot_id = %d''' % (credDict["db_name"] +'.'+credDict["tbl_name3"] , 
                                                                  credDict["db_name"] +'.'+credDict["tbl_name2"], slot_id)
        objdb.InsertQuery_new(sql_query)
    except:
        pass
    
    #Extracting data for PreProcessing
    print("PreProcessing the Data")
    try:
        sql_query = '''SELECT tweet_id, pre_text_str from %s where processed_flag = "No"''' % (credDict["db_name"] +'.'+credDict["tbl_name3"])
        data = objdb.ExtractMultipleQuery(sql_query)
        preprocDF = PreProcTuple_To_Dataframe(data)
        preprocDF['text'] = preprocDF['text'].apply(lambda x: pd.Series(TextPreprocess(x)))
        bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)
        sleep(1)
        for i in range(len(preprocDF)):
            tweet_id = preprocDF['tweet_id'].iloc[i]
            text = preprocDF['text'].iloc[i]
            sql_query = ''' UPDATE %s
            set pre_text_str = "%s" , processed_flag = "Yes"
            where tweet_id = "%s"''' % (credDict["db_name"] +'.'+credDict["tbl_name3"] , text, tweet_id )
            try:
                objdb.InsertQuery_new(sql_query)
            except:
                continue
            bar.update(i)
    except:
        print("error")
        
    # Insert the tweet_ids into Sentiment & Categories Table based on the 
    try:
        sql_query = '''INSERT INTO %s(tweet_id) 
        Select tweet_id from %s where slot_id = %d''' % (credDict["db_name"] +'.'+credDict["tbl_name4"] , 
                                                                  credDict["db_name"] +'.'+credDict["tbl_name3"], slot_id) 
        objdb.InsertQuery_new(sql_query)
    except:
        print("Error : Sentiment Table : Slot_id : %d") %(slot_id)
    try:
        sql_query = '''INSERT INTO %s(tweet_id) 
        Select tweet_id from %s where slot_id = %d''' % (credDict["db_name"] +'.'+credDict["tbl_name5"] , 
                                                                  credDict["db_name"] +'.'+credDict["tbl_name3"], slot_id) 
        objdb.InsertQuery_new(sql_query)
    except:
        print("Error : Categories Table : Slot_id : %d") %(slot_id)

# -----------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------- 
# -----------------------------------------------------------------------------------------------------------------
    print("")    
    print("Fetching data for the Sentiment/Categories Tables")
    Error_flag = 0
    # Find the Pkg_value for sentiment table. Part of Sentiment Analysis
    # Find the Category for the tweet. Part of Category Assessment
    try:
        sql_query = '''select s.tweet_id, p.pre_text_str, s.Pkg_Value, c.ambiance,c.menu, c.service, c.general from %s s join %s c join %s p 
        where s.tweet_id = c.tweet_id and s.tweet_id = p.tweet_id and 
        ( s.Pkg_Value is null OR c.ambiance is null )''' % (credDict["db_name"] +'.'+credDict["tbl_name4"],  # Sentiment
           credDict["db_name"] +'.'+credDict["tbl_name5"],  # Categories
           credDict["db_name"] +'.'+credDict["tbl_name3"])  # PreProcessed_Data
#        print(sql_query)
        data = objdb.ExtractMultipleQuery(sql_query)
        SentCatDF = SentCatTuple_To_Dataframe(data)
        SentCatDF['pkgval'] = SentCatDF['text'].apply(lambda x: pd.Series(LabelFunction_2.sentimentAvg(LabelFunction_2.packages(x))))
        dWords = dict()
        files = ['all_menus.txt', 'ambiance.txt', 'service.txt']
        cat = ['food', 'ambiance', 'service']

        dWords = Categorize_v2.loadCategoryWords(files, dWords, cat)
        SentCatDF[['ambiance','menu', 'service', 'general']] = SentCatDF['text'].apply(lambda x: pd.Series([Categorize_v2.categorizeTweet(x,dWords)[0],
                                                                                                             Categorize_v2.categorizeTweet(x,dWords)[1],
                                                                                                            Categorize_v2.categorizeTweet(x,dWords)[2],
                                                                                                            Categorize_v2.categorizeTweet(x,dWords)[3]]))
    except:
        Error_flag = 1
        print("Error: Fetching data for the Sentiment/Categories Tables")
    if Error_flag == 0:
        try:
            print("Populating data in the Sentiment/Categories Tables")
            bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)
            sleep(1)
            for i in range(len(SentCatDF)):
                tweet_id = SentCatDF['tweet_id'].iloc[i]
                pkgval = SentCatDF['pkgval'].iloc[i]
                ambiance = SentCatDF['ambiance'].iloc[i]
                menu = SentCatDF['menu'].iloc[i]
                service = SentCatDF['service'].iloc[i]
                general = SentCatDF['general'].iloc[i]
                sql_query_1 = ''' UPDATE %s
                set Pkg_Value = %s
                where tweet_id = "%s"''' % (credDict["db_name"] +'.'+credDict["tbl_name4"] , pkgval, tweet_id )            
                sql_query_2 = ''' UPDATE %s
                set ambiance = %s, menu = %s, service = %s, general = %s
                where tweet_id = "%s"''' % (credDict["db_name"] +'.'+credDict["tbl_name5"], ambiance, menu, service, general, tweet_id )
                try:
                    objdb.InsertQuery_new(sql_query_1)
                    objdb.InsertQuery_new(sql_query_2)
                except:
                    continue          
                bar.update(i)
        except:
            print("Error: Populating data in the Sentiment/Categories Tables")
    
                                                                                        
# This is the starting point
if __name__ == "__main__":
	main("CredLoc/cred_file.txt")