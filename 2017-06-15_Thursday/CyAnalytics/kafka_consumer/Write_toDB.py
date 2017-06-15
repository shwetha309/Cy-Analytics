import json
import psycopg2
from kafka import KafkaConsumer

def connect_db():
	try:
    		conn = psycopg2.connect("dbname='attacksdb' user='postgres' host='ip-10-0-0-6' password='insight'")
    		return conn
	except:
   		 print "I am unable to connect to the database"

def write_table(conn,rows):
	q = """ insert into attack_records values (%s,%s,%s,%s,%s,%s,%s) """
	cur = conn.cursor()
	cur.executemany(q, rows)
    	conn.commit()
	print "Committed"
	
	#	print "Failed"
    	#	conn.rollback()

def main():

	
	consumer = KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii').decode('utf-8')))
	consumer.subscribe('ActivityWithinRegionStreams')

	rows=[]
	for msg in consumer:
		print msg		
		row=[long(msg.value['timestamp']),str(msg.value['attack_type']),str(msg.value['attack_subtype']),float(msg.value['latitude']),float(msg.value['longitude']),str(msg.value['city']),str(msg.value['country'])]
		
		rows.append(row)
		print len(rows)
		if len(rows) > 10:
			#print rows
			conn = connect_db()
			write_table(conn,rows)
			rows=[]

main()
