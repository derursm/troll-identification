from datetime import datetime 
import timedelta
import mysql.connector
# own functions
from timedelta import timeDelta 

try:
	### Read from MySQL DB ### 

	dbcon = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='ch')
	c = dbcon.cursor()

	#read from db
	c.execute("DROP VIEW platform_commentators;")
	c.execute("CREATE VIEW platform_commentators AS (SELECT comment.id AS c_id, substr(comment.timestamp,1,25) AS c_timestamp, comment.author AS commentators, comment.comment_text AS c_text, platform.id AS p_id, platform.title AS p_t, article.id AS a_id, article.title AS a_t, substr(article.timestamp,1,25) AS a_timestamp FROM ch.article, ch.comment, ch.platform WHERE platform.id = article.platform_id AND article.id = comment.article_id);")
	c.execute("SELECT c_id, c_timestamp, commentators, a_id, a_timestamp, p_id FROM platform_commentators WHERE c_id <= 839234 AND c_id IS NOT NULL;") #839234

	rows = c.fetchall()
	c.close()
	dbcon.close()

	# get a lits of the time stamps of one 
	def timestampCheck():
		tmp_commentator_list = list()
		tmp_platform = list()
		skip_it = False

		print("\n LET S GO \n")
		for idx, item in enumerate(rows):  
			current_commentator = item[2]
			current_platform = item[5]		# Do not use this if you do not want to distinguish between platforms
			
			if current_commentator in tmp_commentator_list and current_platform in tmp_platform:
				skip_it = True
				continue
			else:
				skip_it = False
				time_commentators = list()
				for idx, item in enumerate(rows):
					if item[2] == current_commentator and item[5] == current_platform:	# CHECK OF COMMENTATOR and PLATFORM ID IS NECESSARY IN HERE 
						time_commentators.append(rows[idx]) # collects all the rows of a commentator per platform 
				
				# got all rows for the current_commentator --> add it to the list to NOT check it again		
				tmp_commentator_list.append(current_commentator)
				tmp_platform.append(current_platform)

				if len(time_commentators) > 5: # the avg number of comments per commentator per platform is 4.54 
					timeDelta(time_commentators)


				###
				# printing the current commentator and its details like 
				# (985, datetime.datetime(2015, 10, 27, 14, 59), 'Skeptiker', 44, '2015-10-27 00:00:00') 	
				###
				#print("The current commentator is: {} \n".format(current_commentator))
				#for i in time_commentators: # in here we have the block of the timestamps belonging to one commentator with comment IDs
				#	print(i, "\n")
				#print("\n\n")

				

	def main():
		timestampCheck()

	main()


except mysql.connector.Error as err: ### error handlnig ### 
	print("Something went wrong: {}".format(err))

finally:
	#print("FINAL LIST")
	#for i in final:
	#	print(i, "\n")

	#files = open('timeCheck.txt','w')
	#for line in str(final):
	#	files.write(line)
	#files.close()
	
	print("\n STOP \n")

	

