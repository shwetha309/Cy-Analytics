import psycopg2


def connect_db():
	try:
    		conn = psycopg2.connect("dbname='attacksdb' user='postgres' host='ip-10-0-0-12' password='insight'")
    		return conn
	except:
   		 print "I am unable to connect to the database"

def read_table(conn):
	cur = conn.cursor()
	cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
	rows=cur.fetchall()
	print rows

def main():
	conn = connect_db()
	read_table(conn)

main()
   
