# -*- coding: utf-8 -*-
"""
Created on Sat Feb  4 13:16:02 2017

@author: kgairola
"""

"""
Comments : 

"""
import pymysql as mysql

class Database_Interaction:
    
    def __init__(self, userId, passId, dbName = "performance_schema", dbhost = "localhost"):
        self.userId = userId
        self.passId = passId 
        self.dbName = dbName
        self.dbhost = dbhost

    def CheckConnection(self):
        try:
            self.db = mysql.connect(self.dbhost, self.userId, self.passId)
            cursor = self.db.cursor()
            cursor.execute("SELECT VERSION()")
            data_ver = cursor.fetchone()
            cursor.close()
            self.db.close()
        except:
            data_ver = ("null",)
        return data_ver
          
    def Connectdb(self, dbName):
        try:
            self.dbName = dbName
            self.db = mysql.connect(self.dbhost, self.userId, self.passId, self.dbName)
            cursor = self.db.cursor()
            cursor.execute("use %s" %(self.dbName))
            db_flag = ("Connected",)
        except:
            db_flag = ("null",)
        return db_flag
            
    def InsertQuery_new(self, sql_query):
        try:
            self.db = mysql.connect(self.dbhost, self.userId, self.passId, self.dbName)
            cursor = self.db.cursor()
            cursor.execute(sql_query)
            self.db.commit()
#            cursor.close()
#            self.db.close()
            return 1
        except:
            #self.db.rollback()
            return -1

    def ExtractOneQuery(self, sql_query):
        self.db = mysql.connect("localhost", self.userId, self.passId, self.dbName)
        cursor = self.db.cursor()
        cursor.execute(sql_query)
        data = cursor.fetchone()
        return data
    
    def ExtractMultipleQuery(self, sql_query):
        try:
            self.db = mysql.connect("localhost", self.userId, self.passId, self.dbName)
            cursor = self.db.cursor()
            cursor.execute(sql_query)
            data = cursor.fetchall()
            return data
        except:
            return "null"
