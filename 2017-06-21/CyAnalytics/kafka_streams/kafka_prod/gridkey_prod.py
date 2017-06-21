import threading, logging, time,datetime

from kafka import KafkaConsumer, KafkaProducer


class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers='52.7.164.216:9092')
        
        #letters= ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']
        while True:
	    with open("/home/ubuntu/merge.txt") as f:
	    	for line in f:
            		producer.send('GridMapCartesian', (line.strip().replace("\"","")))
            		#producer.send('Letters', b"\xc2Hola, mundo!")
           	time.sleep(1)

def main():
	producer = Producer()
   	producer.start()
    	while True:
		time.sleep(10)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
