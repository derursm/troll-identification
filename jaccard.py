from __future__ import division
import itertools
import re
import numpy as np
import sqlite3 as lite

class Jaccard:
	
	#k: word-grams
	#t: threshold
	#m: minimum tokens
	def __init__(self,k,t,m,db):
		self.k=k
		self.t=t
		self.m=m
		self.shingleLength=[]
		self.db=db
		
	def makeASetOfTokens(self,comment):
		#remove URLs, "Der Kommentar, auf den Sie Bezug nehmen, wurde bereits entfernt."
		# replace non-alphanumeric char with a space
		withoutURL=re.sub("(?:www|http?)[^\s]+", " ",  comment)
		withoutCommentRemoved=withoutURL.replace("Der Kommentar, auf den Sie Bezug nehmen, wurde bereits entfernt."," ")
		tokens = re.sub("[^(\w|\xe4|\xfc|\xf6|\xdf)]", " ",  withoutCommentRemoved.lower()).split()
		sh = set()
		for i in range(len(tokens)-self.k+1):
			t = tokens[i]
			for x in tokens[i+1:i+self.k]:
				t += ' ' + x 
			sh.add(t)
		self.shingleLength.append(len(sh))
		return sh

	def createShingles(self,comments):
		shingles = []
		for comment in comments:
			sh = self.makeASetOfTokens(comment)
			shingles.append(sh)
		return shingles

	def createShingles2(self,comments):
		shingles = []
		for comment in comments:
			sh = self.makeASetOfTokens(comment[0])
			shingles.append(sh)
		return shingles
			

	def jaccard_set(self,s1, s2):
		u = s1.union(s2)
		i = s1.intersection(s2)
		if len(u)>0:
			return len(i)/len(u)
		else:
			return 0

	def calculateJaccard(self,items,ids,comments,articleIds,commentators,platforms):
		shingles = self.createShingles(comments)
		length=len(items)
		for i1 in range(length):
			print(i1)
			if self.shingleLength[i1]>self.m:
				minShingle=self.shingleLength[i1]*self.t
				maxShingle=self.shingleLength[i1]/self.t
				for i2 in range(i1,length):
					if self.shingleLength[i2]>self.m:
						if self.shingleLength[i2]>=minShingle and self.shingleLength[i2]<=maxShingle:
							if i1!=i2:
								if articleIds[i1] != articleIds[i2]:
									sim = self.jaccard_set(shingles[i1], shingles[i2])
									if sim>self.t:
										#c=[ids[i1],ids[i2]]
										#print(("%s : jaccard=%s") %(c,sim))
										#print("Com1: "+comments[i1])
										#print("Com2: "+comments[i2])
										#print('\n')
										self.db.addJaccard(commentators[i1],platforms[i1],sim,ids[i1],ids[i2],platforms[i2])
										self.db.addJaccard(commentators[i2],platforms[i2],sim,ids[i2],ids[i1],platforms[i1])

	def calculateJaccardOnOwnCommentsForAll(self):
                commentators = self.db.getAllEntries()
                i=1
		for commentator in commentators:
                        print(i)
                        i=i+1
                        sim=0
                        commentsCount=0
                        con = lite.connect('database.sqlite')
                        with con:
                                cur = con.cursor()
                                cur.execute("Select comment.comment_text from comment,article,platform where comment.article_id=article.id AND article.platform_id=platform.id AND comment.author=? AND platform.title=?",(commentator['commentator'],commentator['platform']))
                                comments = cur.fetchall()
                                commentsCount = len(comments)
                                shingles = self.createShingles2(comments)
                                for i1 in range(commentsCount):
                                        for i2 in range(i1,commentsCount):
                                                if i1!=i2:
                                                        sim=sim + (self.jaccard_set(shingles[i1], shingles[i2]))
                        if commentsCount>1:
                                self.db.addTrollMetric(commentator['commentator'],commentator['platform'],'jaccard_OwnSimilarity',sim/((commentsCount*(commentsCount-1))/2))
                        self.db.addTrollMetric(commentator['commentator'],commentator['platform'],'jaccard_TotalNumber',commentsCount)                          
                        

	def calculateAndStoreTotalNumberOfJaccardSimilarities(self):
		commentators = self.db.getAllEntries()
		i=1
		for commentator in commentators:
                        print(i)
                        i=i+1
			comments=commentator['comments']
			duplicates=0
			totalNumber=0
			ownCommentIds=set()
			similarcommentIds=set()
			for comment in comments:
				ownCommentIds.add(comment['c_id'])
			for comment in comments:
				jaccardSimilarityList=comment['jaccard_similarity_list']
				for jaccardSimilarity in jaccardSimilarityList:
					similarcommentIds.add(jaccardSimilarity['cc_id'])
				if len(set(similarcommentIds).intersection(set(ownCommentIds)))>0:
					duplicates=duplicates+1
			totalNumber=len(ownCommentIds.union(similarcommentIds))
			self.db.addTrollMetric(commentator['commentator'],commentator['platform'],'jaccard_TotalNumber',totalNumber)
			self.db.addTrollMetric(commentator['commentator'],commentator['platform'],'jaccard_#Comments_with_similar_comments',len(ownCommentIds))
			self.db.addTrollMetric(commentator['commentator'],commentator['platform'],'jaccard_#Comments_with_similar_own_comments',duplicates)

	def calculateAndStoreJaccardSimilarityScore(self):
		commentators = self.db.getAllEntries()
		scoreList=[]
		for commentator in commentators:
			metrics=commentator['troll_metrics']
			score=metrics['jaccard_TotalNumber']
			scoreList.append(score)
				
		self.calculateQuantiles(scoreList)
		
		commentators = self.db.getAllEntries()
		for commentator in commentators:
			score=0
			metrics=commentator['troll_metrics']
			score=metrics['jaccard_TotalNumber']
			if score!=0:
				t_count=0
				if score <  self.lower_quartile_nmb: 
					t_count += 0.1
				elif score <= self.mean_nmb:
					t_count += 0.3
				elif score > self.mean_nmb and score <= self.neunziger_quartile_nmb:
					t_count += 0.5
				elif score > self.neunziger_quartile_nmb and score <= self.whisker_nmb:
					t_count += 0.7
				elif score > self.whisker_nmb:
					t_count += 0.9
				self.db.addTrollMetric(commentator['commentator'],commentator['platform'],"jaccard_SimilarityScore",t_count)
				self.db.addTrollMetric(commentator['commentator'],commentator['platform'],"jaccard_SimilarityPercentage",str((score/self.mean_nmb)*100))

	def calculateQuantiles(self,scoreList):
		withoutZeroValues = [s for s in scoreList if s != 0]
		self.mean_nmb = np.mean(withoutZeroValues)
		self.lower_quartile_nmb = np.percentile(withoutZeroValues, 25, axis=None, interpolation='nearest')
		self.neunziger_quartile_nmb = np.percentile(withoutZeroValues, 90, axis=None, interpolation='nearest')
		self.whisker_nmb = np.percentile(withoutZeroValues, 95, axis=None, interpolation='nearest')
				

