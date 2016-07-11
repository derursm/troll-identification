import sqlite3 as lite
import numpy as np

class QuantityComments:

        def __init__(self,db):
                self.db=db

        def loadData(self):
        ### Read from MySQL DB ### 
                con = lite.connect('database.sqlite')
                with con:
                        cur = con.cursor()
                        cur.execute("Select comment.author, platform.title, count(comment.comment_text) from comment, article, platform where comment.article_id=article.id and platform.id = article.platform_id GROUP BY comment.author, platform.title")
                        return cur.fetchall()
                                                                
        ### Finished reading form MySQL DB ###

        def addQuantity(self,result):
                for r in result:
                        self.db.addTrollMetric(r[0],r[1],"Number_of_Comments",r[2])

        def commentQuantityMetrics(self,query3):
                tmp_nmb_commentator = list([x[0] for x in query3])
                tmp_nmb_platform = list([x[1] for x in query3])
                tmp_nmb_comments = list([x[2] for x in query3])
                
                mean_nmb = np.mean(tmp_nmb_comments)
                minimum_nmb = min(tmp_nmb_comments)
                lower_quartile_nmb = np.percentile(tmp_nmb_comments, 25, axis=None, interpolation='nearest')
                median_nmb = np.median(tmp_nmb_comments)
                achtziger_quartile_nmb = np.percentile(tmp_nmb_comments, 80, axis=None, interpolation='nearest')
                neunziger_quartile_nmb = np.percentile(tmp_nmb_comments, 90, axis=None, interpolation='nearest')
                whisker_nmb = np.percentile(tmp_nmb_comments, 99, axis=None, interpolation='nearest')
                maximum_nmb = max(tmp_nmb_comments)

                #print(mean_nmb)
                #print(neunziger_quartile_nmb)
                #print(whisker_nmb)
                #print(maximum_nmb)

                return (mean_nmb,lower_quartile_nmb,neunziger_quartile_nmb,whisker_nmb,maximum_nmb)


        def quantityComments(self,mean_nmb, lower_quartile_nmb, neunziger_quartile_nmb, whisker_nmb, maximum_nmb,query3):
                tmp_commentator = list()
                tmp_platform = list()

                skip_it = False

                for idx, item in enumerate(query3):
                        current_commentator = item[0]
                        current_platform = item[1]

                        if current_commentator in tmp_commentator and current_platform in tmp_platform:
                                skip_it = True
                                continue
                        else:
                                skip_it = False
                                bag_of_lines = list()
                                for idx, item in enumerate(query3):
                                        if item[0] == current_commentator and item[1] == current_platform:
                                                bag_of_lines.append(query3[idx])

                                tmp_commentator.append(current_commentator)
                                tmp_platform.append(current_platform)

                                quantity_set = list()
                                t_count = 0
                                # set with the current commentator of platform to compute the avg. length of 
                                for idx, item in enumerate(bag_of_lines):
                                        quantity_set.append(item[2])
                                        quantity_check = item[2]

                                        if quantity_check <  lower_quartile_nmb: 
                                                t_count += 0.1
                                        elif quantity_check <= mean_nmb:
                                                t_count += 0.3
                                        elif quantity_check > mean_nmb and quantity_check <= neunziger_quartile_nmb:
                                                t_count += 0.5
                                        elif quantity_check > neunziger_quartile_nmb and quantity_check <= whisker_nmb:
                                                t_count += 0.7
                                        elif quantity_check > whisker_nmb:
                                                t_count += 0.9

                                #print("Current Commentator: ", current_commentator, "Current Platform: ", current_platform, "T Count: ", t_count)
                                
                                # MongoDB connection
                                self.db.addTrollMetric(current_commentator,current_platform,"Number_of_Comments_Score",t_count)
                                self.db.addTrollMetric(current_commentator,current_platform,"Number_of_Comments",quantity_check)
                                

        def main(self):
                querryResult=self.loadData()
                mean_nmb,lower_quartile_nmb,neunziger_quartile_nmb,whisker_nmb,maximum_nmb = self.commentQuantityMetrics(querryResult)
                self.quantityComments(mean_nmb, lower_quartile_nmb, neunziger_quartile_nmb, whisker_nmb, maximum_nmb,querryResult)
