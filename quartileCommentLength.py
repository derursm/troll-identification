import mysql.connector
import numpy as np
from pymongo import MongoClient

try:
	### Read from MySQL DB ### 

	dbcon = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='ch')
	
	c = dbcon.cursor()
	c.execute("DROP VIEW commentsPerCommentator")
	c.execute("CREATE VIEW commentsPerCommentator AS (SELECT commentators, sum(length(c_text)) as sum_text_length, count(*) as comments FROM platform_commentators GROUP BY commentators);")
	c.execute("SELECT commentators, sum_text_length/comments as avg_comment_length from commentsPerCommentator;")
	rows = c.fetchall()
	c.close()
	
	c = dbcon.cursor()
	c.execute("DROP VIEW platform_commentators;")
	c.execute("CREATE VIEW platform_commentators AS (SELECT comment.id AS c_id, substr(comment.timestamp,1,25) AS c_timestamp, comment.author AS commentators, comment.comment_text AS c_text, platform.id AS p_id, platform.title AS p_t, article.id AS a_id, article.title AS a_t, substr(article.timestamp,1,25) AS a_timestamp FROM ch.article, ch.comment, ch.platform WHERE platform.id = article.platform_id AND article.id = comment.article_id);")
	c.execute("SELECT commentators, c_text, p_id, p_t FROM platform_commentators WHERE c_id <= 1243427 AND c_id IS NOT NULL;")
	lines = c.fetchall()
	c.close()
	### Finished reading form MySQL DB ### 

	### General Mean, Median and Quartiles ### 
	def commentLengthMetrics():
		tmp_list = list()
		for idx, item in enumerate(rows):
			tmp_list.append(item[1])

		tmp_np_list = np.asarray(tmp_list)
		median_len = np.median(tmp_np_list)
		mean_len = np.mean(tmp_np_list)
		minimum_len = min(tmp_np_list)
		lower_quartile_len = np.percentile(tmp_np_list, 25, axis=None, interpolation='nearest')
		middle_quartile_len = np.percentile(tmp_np_list, 50, axis=None, interpolation='nearest')
		upper_quartile_len = np.percentile(tmp_np_list, 75, axis=None, interpolation='nearest')
		achtziger_quartile_len = np.percentile(tmp_np_list, 80, axis=None, interpolation='nearest')
		neunziger_quartile_len = np.percentile(tmp_np_list, 90, axis=None, interpolation='nearest')
		maximum_len = max(tmp_np_list)

		#print("75: ", upper_quartile_len)
		#print("80: ", achtziger_quartile_len)
		#print("90: ", neunziger_quartile_len)
		#print("Max: ", maximum_len)

		return neunziger_quartile_len

	neunziger_quartile_len = commentLengthMetrics()	
	### END general metrics ### 

	### avg length computation per commentator per platform ### 
	def commentLength(neunziger_quartile_len):
		tmp_commentator = list()
		tmp_platform = list()

		skip_it = False

		for idx, item in enumerate(lines):
			current_commentator = item[0]
			current_platform = item[3]

			if current_commentator in tmp_commentator and current_platform in tmp_platform:
				skip_it = True
				continue
			else:
				skip_it = False
				bag_of_lines = list()
				for idx, item in enumerate(lines):
					if item[0] == current_commentator and item[3] == current_platform:
						bag_of_lines.append(lines[idx])

				tmp_commentator.append(current_commentator)
				tmp_platform.append(current_platform)
				

				# Comment length troll check 
				length_set = list()
				c_count = 0
				t_count = 0
				# set with the current commentator of platform to compute the avg. length of 
				for idx, item in enumerate(bag_of_lines):
					c_count += 1 
					length_set.append(len(item[1]))
					len_check = len(item[1])

					if len_check > neunziger_quartile_len: # each comment longer than 790 character
						t_count += 0.1
					
					
				average_comment_len = sum(length_set)/c_count
				print(current_commentator, "Current Platform: ", current_platform, "AVG: ", average_comment_len, "T Count: ", t_count)

				# MongoDB connection 
				client = MongoClient('localhost', 27017)
				mydb = client.trolldata
				myrecord = {
					"commentator": current_commentator,
					"platform": current_platform,
					"avg_length": t_count
					}

				record_id = mydb.trollcommentquantity.insert(myrecord)



	def main():
		commentLengthMetrics()
		commentLength(neunziger_quartile_len)


	main()


except mysql.connector.Error as err: ### error handlnig ### 
	print("Something went wrong: {}".format(err))

finally:
	#save it to a file 
	print("STOP")

	# close DB connection 
	dbcon.close()
