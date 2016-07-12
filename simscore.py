#http://scikit-learn.org/stable/tutorial/text_analytics/working_with_text_data.html

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from nltk.corpus import stopwords
import numpy as np

# DB connectors
import mysql.connector
from pymongo import MongoClient
import pymongo as pym


try:
	### Read from MySQL DB ### 

	dbcon = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='ch')
	c = dbcon.cursor()

	#read from db
	c.execute("DROP VIEW platform_commentators;")
	c.execute("CREATE VIEW platform_commentators AS (SELECT comment.id AS c_id, substr(comment.timestamp,1,25) AS c_timestamp, comment.author AS commentators, comment.comment_text AS c_text, platform.id AS p_id, platform.title AS p_t, article.id AS a_id, article.title AS a_t, substr(article.timestamp,1,25) AS a_timestamp FROM ch.article, ch.comment, ch.platform WHERE platform.id = article.platform_id AND article.id = comment.article_id);")
	c.execute("SELECT c_id, c_timestamp, commentators, c_text, a_id, a_timestamp, p_id FROM platform_commentators WHERE c_id <= 1243427 AND c_id IS NOT NULL;")
	
	rows = c.fetchall()
	c.close()
	dbcon.close()
	
	c_ids = list([x[0] for x in rows]) 			#getting the ids in a sorted order
	c_timestamp = list([x[1] for x in rows]) 		# getting the timestamps in the sorted order
	commentators = list([x[2] for x in rows]) 	#getting the commentators nicknames in sorted order
	train_set = list([x[3] for x in rows]) 		#extracting the comments of the commentators
	p_ids =	list([x[6] for x in rows]) 		#extracting the p_id of the comment

	test_set = np.asarray(train_set[0:1]) #Query to array

	### creating the sets and close the db conection ###

	#stopWords = stopwords.words('german')

	#count_vectorizer = CountVectorizer(input = train_set, analyzer = 'word', stop_words = stopWords, min_df=1) #min_df between 0 and 1
	#count_vectorizer.fit_transform(train_set)
	#print ("\n Vocabulary: \n", count_vectorizer.vocabulary)

	###	the upper part is not required as long as the stop words are already rejected in the database ###
	#### probably the necessary part ####
	###								  ###

	tfidf_vectorizer = TfidfVectorizer()
	tfidf_matrix = tfidf_vectorizer.fit_transform(train_set) # HOW TO STORE THE VECTORS FOR OLD COMMENTS AND ADD NEW ONES ???

	print("reached mongo write")
	client = MongoClient('localhost', 27017)
	mydb = client.trolldata
	myrecord = {tfidf_matrix}
	record_id = mydb.tfidf.insert(myrecord)
	print("finished mongo write")
	
	tfidf_index = tfidf_matrix.shape[0]
	print("TFIDF Index: \n", tfidf_index) # length of the matrix


########
########
########


	print("\n starting with cosS computation \n")
	cosS = cosine_similarity(tfidf_matrix[0:tfidf_index], tfidf_matrix)  # We need to keep the c_id, commentator and p_id 
	#print("\n Cosine Similarity \n", cosS[0:4])


	for idx, item in enumerate(cosS):
		#index = idx + 1 
		comment_sim_score = list([x[idx] for x in cosS]) #returns the regarding element of the cosine similarity matrix
		comment_id_sim_list = list(zip(c_ids, commentators,comment_sim_score, p_ids)) #combines the comment_ids with the regarding sim_score
		#print("Comment and Sim Score: \n", type(comment_id_sim_list), "\n \n", comment_id_sim_list)

		similar_comments = list()
		for idx, item in enumerate(comment_id_sim_list):
			if item[2] >= 0.3: #adjust the threashold to achieve a higher similarity score
				similar_comments.append(comment_id_sim_list[idx])
						
		c_to_check.append(similar_comments)
		
		#print("\n\n The similar comments for comment {}: (Index and simScore)\n".format(index), similar_comments)
		#np.savetxt("similar_comments_c{}.csv".format(index), similar_comments, delimiter=";") # do not save it in UTF-8 maybe ASCII (geringerer Speicher)

########
########
########


except mysql.connector.Error as err: ### error handlnig ### 
	print("Something went wrong: {}".format(err))

finally:
	#save it to a file 
	print("STOP")

######
# choosing id < 300000  lead to 34 572 Comments take 27 min 46 seconds for computing the list of similar comments
###### 
