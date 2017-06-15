import json,msgpack,datetime
import psycopg2
from kafka import KafkaConsumer

def connect_db():
	try:
    		conn = psycopg2.connect("dbname='attacksdb' user='postgres' host='ip-10-0-0-12' password='insight'")
    		return conn
	except:
   		 print "I am unable to connect to the database"

def write_table(conn,row):
	q = """ insert into hourly_activity values (%s,%s,%s) """
	cur = conn.cursor()
	cur.execute(q, row)
    	conn.commit()
	print "Committed"

def main():
	consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
 #                                auto_offset_reset='earliest')
	consumer.subscribe("HourlyCountStream")

	#rows=[]
	for msg in consumer:
		data = msg.value.split("##")
		time_ts = ((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)
		
		row = [time_ts, data[0], data[1]]		
	#	rows.append(row)
		print row		
		conn = connect_db()
		write_table(conn,row)
		

main()
