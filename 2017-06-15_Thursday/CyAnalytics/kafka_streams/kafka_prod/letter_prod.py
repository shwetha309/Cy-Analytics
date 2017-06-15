import datetime
import time

letters= ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']

while(1):
	for ele in letters:
		ts = datetime.datetime.now()
		print str(ts) + " " + ele
		time.sleep(2)
