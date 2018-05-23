#!/usr/bin/env python
import threading, logging
import multiprocessing
import csv
import sys

from kafka import KafkaProducer


class Producer(threading.Thread):
    def __init__(self, filename):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.filename = filename
        
    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        url_col = -1
        i = -1
        with open(self.filename, 'rU') as csvfile:
            reader = csv.reader(csvfile,delimiter=',')
            for row in reader:
                i += 1
                if i == 0:
                    try:
                        url_col = row.index("URL")
                    except ValueError:
                        print("ERROR: The csv must have a column header titled 'URL'")
                        return
                    continue
                producer.send('demo.incoming', '{"url": "'+row[url_col]+'", "appid":"testapp", "crawlid":"' + self.filename + "_" +str(i)+'", "spiderid":"parsing_link", "maxdepth": 2}')
        producer.close()


def main():
    csv_file_name = ""
    if len(sys.argv) <= 1:
        print("ERROR: You must pass in the csv filename as an argument")
        return
    else:
        csv_file_name = sys.argv[1]

    tasks = [
        Producer(csv_file_name)
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
