#!/usr/bin/env python
import threading, logging, time
import multiprocessing
import csv

from kafka import KafkaConsumer, KafkaProducer


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        
    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        i = -1
        with open('micro-sample_Apr17_rev3.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile,delimiter=',')
            for row in reader:
                i += 1
                if i == 0:
                    continue
                producer.send('demo.incoming', '{"url": "'+row[3]+'", "allowed_domains":["'+row[3]+'"], "appid":"testapp", "crawlid":"micro-sample_Apr17_rev3'+str(i)+'", "spiderid":"parsing_link"}')
                print(i)
                time.sleep(0.1)
        producer.close()

class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        
    def stop(self):
        self.stop_event.set()
        
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='kafka:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['demo.incoming'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
        break
                if self.stop_event.is_set():
                    break

        consumer.close()
        
        
def main():
    tasks = [
        Producer()
        # Consumer()
    ]

    for t in tasks:
        t.start()

    num_rows = 0
    with open('micro-sample_Apr17_rev3.csv', 'rb') as csvfile:
        num_rows = sum(1 for row in csvfile)

    # time.sleep(num_rows / 0.1 + 15)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()
        
        
if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
