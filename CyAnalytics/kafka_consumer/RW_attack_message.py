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
	q = """ insert into attack_message values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
	cur = conn.cursor()
	cur.executemany(q, rows)
    	conn.commit()
	print "Committed"
	
	#	print "Failed"
    	#	conn.rollback()

def main():

	
	consumer = KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii').decode('utf-8')))
	consumer.subscribe('Attack_Message')

	rows=[]
	for msg in consumer:
		print msg		
		row=[long(msg.value['timestamp']),str(msg.value['attack_type']),str(msg.value['attack_subtype']),float(msg.value['latitude']),float(msg.value['longitude']),str(msg.value['city_target']),str(msg.value['country_target']),str(msg.value['attacker_ip']), str(msg.value['attacker']),float(msg.value['latitude2']),float(msg.value['longitude2']),str(msg.value['city_origin']),str(msg.value['country_origin'])]
		
		rows.append(row)
		print len(rows)
		if len(rows) > 1000:
			#print rows
			conn = connect_db()
			write_table(conn,rows)
			rows=[]

main()
