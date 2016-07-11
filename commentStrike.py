import sqlite3 as lite
from pymongo import MongoClient
import numpy as np

class CommentStrike:

    def __init__(self,db):
        self.db = db

    def getArticleIds(self):
        con = lite.connect('database.sqlite')
        with con:
            cur = con.cursor()
            cur.execute("Select id from article")
            return cur.fetchall()

    def main(self):
        self.prepare()
        articleIds = self.getArticleIds()
        for articleId in articleIds:
            con = lite.connect('database.sqlite')
            with con:
                cur = con.cursor()
                print(articleId[0])
                cur.execute("Select author, platform, timestamp from comment_strike where article_id=? Order by timestamp ASC",(str(articleId[0]),))
                comments=cur.fetchall()
                i=1
                for comment in comments:
                    user = self.db.getUser(comment[0],comment[1])
                    metric = user[0]['troll_metrics']
                    positionNumber=metric['comment_strike_Total_Position_Number']    
                    positionNumber=positionNumber+i

                    self.db.db.update({'commentator':comment[0],'platform':comment[1]},{'$inc':{'troll_metrics.comment_strike_counter':1}})
                    self.db.db.update({'commentator':comment[0],'platform':comment[1]},{'$set':{'troll_metrics.comment_strike_Total_Position_Number':positionNumber}})
                    if i<11:
                        self.db.db.update({'commentator':comment[0],'platform':comment[1]},{'$inc':{'troll_metrics.comment_strike_counter_Top10':1}})

                    i=i+1

    def prepare(self):
        con = lite.connect('database.sqlite')
        with con:
            cur = con.cursor()
            cur.execute("drop view comment_strike")
            cur.execute("Create view comment_strike as Select comment.id, comment.author, comment.timestamp, comment.article_id, platform.title as platform from comment,article,platform where comment.article_id=article.id AND article.platform_id=platform.id")

        commentators = self.db.getAllEntries()
        for commentator in commentators:
            self.db.db.update({'commentator':commentator['commentator'],'platform':commentator['platform']},{'$set':{'troll_metrics.comment_strike_counter':0}})
            self.db.db.update({'commentator':commentator['commentator'],'platform':commentator['platform']},{'$set':{'troll_metrics.comment_strike_Total_Position_Number':0}})               
            self.db.db.update({'commentator':commentator['commentator'],'platform':commentator['platform']},{'$set':{'troll_metrics.comment_strike_counter_Top10':0}})

    def commentStrikAverage(self):
        commentators = self.db.getAllEntries()
        for commentator in commentators:
            metric = commentator['troll_metrics']
            positionNumber=metric['comment_strike_Total_Position_Number']
            counter=metric['comment_strike_counter']
            self.db.db.update({'commentator':commentator['commentator'],'platform':commentator['platform']},{'$set':{'troll_metrics.comment_strike_average':(positionNumber/counter)}})

    def calculateAndStoreCommentStrikeScores(self):
        commentators = self.db.getAllEntries()
        scoreList=[]
        for commentator in commentators:
            metrics=commentator['troll_metrics']
            score=0
            try:
                score=metrics['comment_strike_average']
            except: 
                pass
            scoreList.append(score)
        self.calculateQuantiles(scoreList)

        commentators = self.db.getAllEntries()
        for commentator in commentators:
            metrics=commentator['troll_metrics']
            score=0
            try:
                score=metrics['comment_strike_average']
            except: 
                pass
            if score !=  0:
                t_count=0
                if score >=  self.higher_quartile_nmb: 
                    t_count += 0.1
                elif score >= self.mean_nmb:
                    t_count += 0.3
                elif score < self.mean_nmb and score >= self.zehner_quartile_nmb:
                    t_count += 0.5
                elif score < self.zehner_quartile_nmb and score >= self.whisker_nmb:
                    t_count += 0.7
                elif score < self.whisker_nmb:
                    t_count += 0.9
                self.db.addTrollMetric(commentator['commentator'],commentator['platform'],"comment_strike_average_score",t_count)


    def calculateQuantiles(self,scoreList):
        withoutZeroValues = [s for s in scoreList if s != 0]
        self.mean_nmb = np.mean(withoutZeroValues)
        self.higher_quartile_nmb = np.percentile(withoutZeroValues, 75, axis=None, interpolation='nearest')
        self.zehner_quartile_nmb = np.percentile(withoutZeroValues, 10, axis=None, interpolation='nearest')
        self.whisker_nmb = np.percentile(withoutZeroValues, 1, axis=None, interpolation='nearest')
