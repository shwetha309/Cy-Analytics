import json,msgpack,datetime
import psycopg2
from kafka import KafkaConsumer

def connect_db():
	try:
    		conn = psycopg2.connect("dbname='attacksdb' user='postgres' host='ip-10-0-0-6' password='insight'")
    		return conn
	except:
   		 print "I am unable to connect to the database"

def write_table(conn,row):
	q = """ insert into attack_type_count values (%s,%s) """
	cur = conn.cursor()
	cur.execute(q, row)
    	conn.commit()
	print "Committed"

def main():
	consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
	consumer.subscribe("AttackTypeCountStream")

	#rows=[]
	for msg in consumer:
		data = msg.value.split("##")
		row = [data[0], data[1]]
		print row		
		conn = connect_db()
		write_table(conn,row)
		

main()
