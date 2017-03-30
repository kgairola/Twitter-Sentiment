#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 15:34:43 2017

@author: n0z
"""
import tweepy
import time
# Autehntication
auth = tweepy.OAuthHandler ('<Token>',
                          '<Token>')
auth.set_access_token ('<Token>',
                      '<Token>')

api = tweepy.API(auth)

# Step 1: Creating a StreamListener
# override tweepy.StreamListenr to add logic to on_status
class MyStreamListener (tweepy.StreamListener):

    def on_status(self, status):
        print (status.text)
    
    def on_data(self, data):
        try:
            with open ('tweets.json', 'a') as f:
                f.write(data)
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            time.sleep(5)
        return True
 
    def on_error(self, status):
        print(status)
        return True

# Step 2: Creating a Stream
myStreamListener = MyStreamListener()
myStream = tweepy.Stream (auth = api.auth, listener=myStreamListener)

# Step 3: Starting a Stream
myStream.filter(track=['iphone'], async=True)

#myStream.disconnect()
