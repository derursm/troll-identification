from mongoConnector import Mongo
from sqliteConnector import sqliteDB
from jaccard import Jaccard
from commetsperlifetime import CommentsPerLifeTime
from quantityComments import QuantityComments
from commentStrike import CommentStrike
  
if __name__ == '__main__':
        #instanciate
        mySqlite = sqliteDB()
        myMongo = Mongo()
        myJaccard = Jaccard(3,0.75,10,myMongo)
        myCommetsPerLifeTime = CommentsPerLifeTime(2,myMongo)
        myQuantityComments = QuantityComments(myMongo)
        myCommentStrike = CommentStrike(myMongo)
  
        #Load Data
        #items= mySqlite.loadData()
        #ids= mySqlite.getIds(items)
        #comments= mySqlite.getComments(items)
        #articleIds= mySqlite.getArticleIds(items)
        #commentators= mySqlite.getCommentators(items)
        #platforms= mySqlite.getPlatforms(items)

        #LoadData Distinct
        #itemsDistinct=mySqlite.loadDistinctData()
        #commentatorDistincts= mySqlite.getCommentatorsDistinct(itemsDistinct)
        #platformsDistinct= mySqlite.getPlatformsDistinct(itemsDistinct)

        #PopulateMongo
        #mySqlite.populateUserIntoMongo(myMongo,commentatorDistincts,platformsDistinct)

        #CalculateJaccard
        #myJaccard.calculateJaccard(items,ids,comments,articleIds,commentators,platforms,myMongo)
        #myJaccard.calculateAndStoreTotalNumberOfJaccardSimilarities()
        #myJaccard.calculateAndStoreJaccardSimilarityScore()
        #myJaccard2 = Jaccard(3,0,5,myMongo)
        #myJaccard2.calculateJaccardOnOwnCommentsForAll()
        
        #CommentsPerLifeTIme
        #myCommetsPerLifeTime.calculateAndStoreCommentsPerLifeTime(commentatorDistincts,platformsDistinct)
        #myCommetsPerLifeTime.calculateAndStoreCommentsPerLifeTimeScores()
        
        #QuantityComments
        #myQuantityComments.main()

        #Import
        #myMongo.importFromDB()
        #myMongo.delteMetric()
        #myMongo.recalc()

        #mySqlite.createView()
        #myCommentStrike.main()
        #myCommentStrike.commentStrikAverage()

        #r=myQuantityComments.loadData()
        #myQuantityComments.addQuantity(r)
