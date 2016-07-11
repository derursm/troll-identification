import sqlite3 as lite

class sqliteDB:
     
    def loadData(self):
        con = lite.connect('database.sqlite')
        with con:
                cur = con.cursor()
                cur.execute("Select comment.id, comment.comment_text, comment.article_id, comment.author, platform.title from comment,article,platform where comment.article_id=article.id AND article.platform_id=platform.id")
                return cur.fetchall()
    
    def getIds(self,items):
            return list([item[0] for item in items])

    def getComments(self,items):
            return list([item[1] for item in items])

    def getArticleIds(self,items):
            return list([item[2] for item in items])

    def getCommentators(self,items):
            return list([item[3] for item in items])

    def getPlatforms(self,items):
            return list([item[4] for item in items])

    def loadDistinctData(self):
        con = lite.connect('database.sqlite')
        with con:
                cur = con.cursor()
                cur.execute("Select distinct comment.author||'-'||+platform.title as authorAndPlatform, comment.author, platform.title  from comment,article,platform where comment.article_id=article.id AND article.platform_id=platform.id")
                return cur.fetchall()
            
    def populateUserIntoMongo(self,mongoDb,commentators,platforms):
            for i in range(len(commentators)):
                    mongoDb.addNewUser(commentators[i],platforms[i])

    def getCommentatorsDistinct(self,items):
            return list([item[1] for item in items])

    def getPlatformsDistinct(self,items):
            return list([item[2] for item in items])

    def createView(self):
        con = lite.connect('database.sqlite')
        with con:
                cur = con.cursor()
                cur.execute("drop view all_data")
                cur.execute("Create view all_data as Select comment.id, comment.comment_text, comment.timestamp, article.timestamp, comment.author, platform.title from comment,article,platform where comment.article_id=article.id AND article.platform_id=platform.id")
