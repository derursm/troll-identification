from datetime import datetime 	#reference to the class
from datetime import timedelta 	#reference to the class
import datetime as dt 			#reference to the module
import timedelta as td 			#reference to the module
import operator
from pymongo import MongoClient
import pymongo as pym

#time_array = [(2, 2016-03-31 13:15:00, 'Skeptiker', 4, '2016-03-30 00:00:00'), (12, 2016-04-18 13:16:00, 'Skeptiker', 1, '2016-04-18 00:00:00'), (68, '2016-03-15 15:22:00', 'Skeptiker', 8, '2016-03-14 00:00:00'), (97, '2016-03-30 14:33:00', 'Skeptiker', 5, '2016-03-29 00:00:00'), (102, '2016-03-17 16:27:00', 'Skeptiker', 6, '2016-03-16 00:00:00'), (110, '2016-04-15 14:45:00', 'Skeptiker', 2, '2016-04-15 00:00:00'), (218, '2016-02-10 16:30:00', 'Skeptiker', 15, '2016-02-09 00:00:00'), (245, '2016-03-03 00:59:00', 'Skeptiker', 9, None), (262, '2016-04-06 22:02:00', 'Skeptiker', 3, '2016-04-06 00:00:00'), (400, '2016-01-19 14:40:00', 'Skeptiker', 18, '2016-01-18 00:00:00'), (427, '2016-02-19 19:40:00', 'Skeptiker', 14, '2016-02-15 00:00:00'), (533, '2016-01-09 18:26:00', 'Skeptiker', 24, '2016-01-09 00:00:00'), (826, '2016-01-11 15:22:00', 'Skeptiker', 24, '2016-01-09 00:00:00'), (985, '2015-10-27 14:59:00', 'Skeptiker', 44, '2015-10-27 00:00:00'), (999, '2015-11-09 15:08:00', 'Skeptiker', 42, '2015-11-08 00:00:00')] 


def timeDelta(time_array):
	# slicing the current array
	c_ids = list([x[0] for x in time_array])
	c_timestamp = list([x[1] for x in time_array])
	commentator = list([x[2] for x in time_array])
	a_id = list([x[3] for x in time_array])
	a_timestamp = list([x[4] for x in time_array])
	p_id = list([x[5] for x in time_array])

	# transform the datetime-string to a datetime object 
	tmp_time_array = list()
	for idx, item in enumerate(c_timestamp):
		tmp_time_array.append(datetime.strptime(item, "%Y-%m-%d %H:%M:%S")) #t1 = datetime.strptime(time_array[i][1], "%Y-%m-%d %H:%M:%S")

	# put the data back together
	time_list = list(zip(c_ids,tmp_time_array,commentator,a_id,a_timestamp, p_id))
	# sort the list by datetime -> default is ascending
	time_list.sort(key=operator.itemgetter(1)) #,reverse=True
	
	#print("Currently analysed commentator: {}".format(time_list[0][2]))
	# now compute the deltas and check if they are in a range 
	time_delta = list()
	for idx, item in enumerate(time_list):
		if idx < len(time_list)-1:
			index = idx +1 
			current_time = item[1]
			compared_time = time_list[idx+1][1]
			result = compared_time - current_time
			#print("{} - {} = {}".format(compared_time, current_time, result))
			time_delta.append(result)

	#return time_delta
	#return time_list 

#time_delta = timeDelta(time_array)
#time_list = timeDelta(time_array)


#def timeTrollLevel(time_list, time_delta):
	spam_counter = 0
	troll_level = 0
	time_threashold = timedelta.total_seconds(timedelta(seconds=240)) #threashold for posting intervall
	#print("Deltas of Comments from {}: \n".format(time_list[0][2]), time_delta, "\n\n")
		
	for item in time_delta:
		item_total_seconds = timedelta.total_seconds(item)
		if spam_counter < 6:
			if item_total_seconds < time_threashold: 
				spam_counter += 1
				troll_level += 0.2
			else:
				spam_counter = spam_counter
				troll_level = troll_level
	
	troll_level_list = list()
	troll_level_list.append((time_list[0][2], time_list[0][5], spam_counter, troll_level))	
	
	# MongoDB connection 
	client = MongoClient('localhost', 27017)
	mydb = client.trolldata
	myrecord = {"trolllevel": troll_level, "spamcounter": spam_counter, "p_id": time_list[0][5], "commentator": time_list[0][2]}
	record_id = mydb.trolltime.insert(myrecord)
	
	print ("{} on platform {}: Spam-Counter {} and Troll-Level {} \n".format(time_list[0][2], time_list[0][5],spam_counter, troll_level)) #posted several comments more than 5 times in under 4 minutes

	#return troll_level_list to MongoDB
	

#def main():
#	timeDelta()
#	timeTrollLevel()

#main()
