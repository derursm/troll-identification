import datetime
import time
import sqlite3 as lite
import itertools
import re
import numpy as np

class CommentsPerLifeTime:

	#m = minimum number of comments
        
	def __init__(self,m,db):
		self.m=m
		self.db=db
          
	def calculateAndStoreCommentsPerLifeTime(self,commentatorsDistinct,platformsDistinct):
		con = lite.connect("database.sqlite")
		with con:       
			#Calculate measures for each user
			i=0
			cur = con.cursor()
			#cur.execute("DROP VIEW platform_commentators")
			#cur.execute("CREATE VIEW platform_commentators AS SELECT comment.timestamp as ts, comment.author as commentator, platform.title as pt from comment, article, platform where comment.article_id=article.id and platform.id = article.platform_id")
			for commentator in commentatorsDistinct:
				print(i)
				i=i+1
				cur.execute("Select ts from platform_commentators where commentator=? and pt=?",(commentator,platformsDistinct[i-1]))
				comments=cur.fetchall()
				timestamps=list([comment[0] for comment in comments])
				count=len(timestamps)
				#avg_interval=datetime.datetime.strptime('00:00:00', '%H:%M:%S')-datetime.datetime(1900,1,1)
				if count>=self.m:
					timestamps.sort()
					first=datetime.datetime.strptime(timestamps[0], '%Y-%m-%d %H:%M:%S')
					last=datetime.datetime.strptime(timestamps[len(timestamps)-1], '%Y-%m-%d %H:%M:%S')
					avg_interval=(last-first)/count
					#self.db.addTrollMetric(commentator,platformsDistinct[i-1],"Life_Time_Comment_Frequency",str(avg_interval))
					#self.db.addTrollMetric(commentator,platformsDistinct[i-1],"Life_Time_Comment_Frequency_Seconds",(int)(avg_interval.total_seconds()))

	def calculateAndStoreCommentsPerLifeTimeScores(self):
		commentators = self.db.getAllEntries()
		scoreList=[]
		for commentator in commentators:
			metrics=commentator['troll_metrics']
			score=0
			try:
                                score=metrics['Life_Time_Comment_Frequency_Seconds']
                        except: 
                                pass
			scoreList.append(score)
		self.calculateQuantiles(scoreList)

		commentators = self.db.getAllEntries()
		for commentator in commentators:
			metrics=commentator['troll_metrics']
			score=0
			try:
				score=metrics['Life_Time_Comment_Frequency_Seconds']
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
                                self.db.addTrollMetric(commentator['commentator'],commentator['platform'],"'Life_Time_Comment_Frequency_Score",t_count)


	def calculateQuantiles(self,scoreList):
		withoutZeroValues = [s for s in scoreList if s != 0]
		self.mean_nmb = np.mean(withoutZeroValues)
		self.higher_quartile_nmb = np.percentile(withoutZeroValues, 75, axis=None, interpolation='nearest')
		self.zehner_quartile_nmb = np.percentile(withoutZeroValues, 10, axis=None, interpolation='nearest')
		self.whisker_nmb = np.percentile(withoutZeroValues, 1, axis=None, interpolation='nearest')
          
          
        


