#!/usr/bin/env python
import threading, logging
import multiprocessing
import csv

from kafka import KafkaProducer


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        
    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        i = -1
        with open('charter_URLs_2016.csv', 'rU') as csvfile:
            reader = csv.reader(csvfile,delimiter=',')
            for row in reader:
                i += 1
                if i == 0 or row[-2] == "0":
                    continue
                producer.send('demo.incoming', '{"url": "'+row[-2]+'", "appid":"testapp", "crawlid":"charter_URLs_2016_'+str(i)+'", "spiderid":"parsing_link", "maxdepth": 2}')
        producer.close()


def main():
    tasks = [
        Producer()
    ]

    for t in tasks:
        t.start()

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

