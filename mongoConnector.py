from pymongo import MongoClient
import datetime
import pymongo
from decimal import *

class Mongo:            
    
    def __init__(self):
        client = MongoClient('localhost:27017')
        db = client.TrollDB
        self.db = db.Troll
    
    def getAllEntries(self):
        cursor = self.db.find().batch_size(30)
        return cursor

    def querry(self,query):
        cursor = self.db.find(query)
        for a in cursor:
            print a

    def getComment(self,commentator,platform,c_id):
        return self.db.find({'commentator':commentator,'platform':platform,'comments.c_id':c_id})

    def getUsersFromPlatform(self,platform):
        return self.db.find({'platform':platform})

    def getUser(self,commentator,platform):
        return self.db.find({'commentator':commentator,'platform':platform})
        
    def addNewUser(self,commentator,platform):
        user= self.getUser(commentator,platform)
        if user.count()==0:
            self.db.insert({
                'commentator': commentator,
                'platform':platform,
                'troll_metrics':{},
                'comments':[]    
                })

    #add or update a metric
    def addTrollMetric(self,commentator,platform,metric,value):
        myString = "troll_metrics."+metric
        self.db.update({'commentator':commentator,'platform':platform},{'$set':{myString: value}})

    def addJaccard(self,commentator,platform,similarity,c_id,cc_id,c_platform):
        if self.getComment(commentator,platform,c_id).count()>0:
            self.addJaccardToExistingUser(commentator,platform,similarity,c_id,cc_id,c_platform)
        else:
            self.addCommentAndJaccardToExistingUser(commentator,platform,similarity,c_id,cc_id,c_platform)
    
    def addCommentAndJaccardToExistingUser(self,commentator,platform,similarity,c_id,cc_id,c_platform):
        self.db.update({'commentator':commentator,'platform':platform},{'$push':{'comments':{
                    'c_id': c_id,
                    'no_jaccard_similar_comments':1,
                    'jaccard_similarity_list': [{
                        'cc_id': cc_id,
                        'c_platform':c_platform,
                        'similarity': similarity
                        }]}}})
        self.db.trolls.update({'commentator':commentator,'platform':platform},{'$inc':{'troll_metrics.no_comments':1}})
                        
    def addJaccardToExistingUser(self,commentator,platform,similarity,c_id,cc_id,c_platform):
        self.db.update({'commentator':commentator,'platform':platform,'comments.c_id':c_id},{'$push':{'comments.$.jaccard_similarity_list':{
                        'cc_id': cc_id,
                        'c_platform':c_platform,
                        'similarity': similarity
                        }}})
        self.db.update({'commentator':commentator,'platform':platform,'comments.c_id':c_id},{'$inc':{'comments.$.no_jaccard_similar_comments':1}})

    def calculateAndStoreOverAllScore(self,metrics,weights):
        users = self.getAllEntries()
        weightCount=Decimal(0)
        for weight in weights:
            weightCount=weightCount+weight
        for user in users:
            score=Decimal(0)
            i=0
            for metric in metrics:
                metricUser = user["troll_metrics"]
                try:   
                    score=score+Decimal((metricUser[metric]*weights[i]))
                except:
                    pass
                i=i+1
            score=score/weightCount
            self.db.update({'commentator':user['commentator'],'platform':user['platform']},{'$set':{'troll_metrics.overall_score': round(score,2)}})

    def cleanUp(self):
        self.db.delete_many({"comments": []})

    def duplicates(self):
        users = self.getAllEntries()
        for user in users:
            entrys = self.db.find({'commentator':user['commentator'],'platform':user['platform']})
            i=0
            for entry in entrys:
                i=i+1
            if i>1:
                print(entry)

    def importFromDB(self):
        client = MongoClient('localhost:27017')
        db2 = client.urs.trolltime_new2
        entries = db2.find(no_cursor_timeout=True)
        for entry in entries:
            self.addTrollMetric(entry['commentator'],entry['p_title'],"trolltime_numb_posts",entry['numb_posts'])
            self.addTrollMetric(entry['commentator'],entry['p_title'],"trolltime_min_intervall",entry['min_intervall'])
            myValue=entry['trolllevel']
            if myValue >5:
                self.addTrollMetric(entry['commentator'],entry['p_title'],"trolltime",1)
            else:
                myValue/5
                self.addTrollMetric(entry['commentator'],entry['p_title'],"trolltime",(round(myValue/5,1)))


    def delteMetric(self):
        users = self.getAllEntries()
        for user in users:
            self.db.update({'commentator':user['commentator'],'platform':user['platform']},{'$unset': {"troll_metrics.'Life_Time_Comment_Frequency_Score":1}})
