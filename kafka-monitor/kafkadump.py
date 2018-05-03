from __future__ import print_function
from __future__ import division
from past.utils import old_div
from kafka import KafkaClient,KafkaConsumer
from kafka.common import NoBrokersAvailable, KafkaUnavailableError

import json
import sys
import traceback
import time
import argparse
import base64

import sqlite3

from scutils.settings_wrapper import SettingsWrapper
from scutils.log_factory import LogFactory
from scutils.method_timer import MethodTimer
from scutils.argparse_helper import ArgparseHelper


def dump_db_store(c, item):
    response_url, url, body = item['response_url'], item['url'], item['body']
    links = " ".join(item['links'])
    appid, crawlid, time_stamp = item['appid'], item['crawlid'], item['timestamp']
    response_headers = str(item['response_headers'])
    request_headers, attrs = str(item['request_headers']), str(item['attrs'])
    status_msg, status_code = item['status_msg'], str(item['status_code'])
    is_pdf = item['is_pdf']
    cur_depth = item["curdepth"]
    
    c.execute("INSERT INTO dump VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (response_url, url,
                            body, links, is_pdf, appid, crawlid, time_stamp, response_headers,
                            request_headers, attrs, status_msg, status_code, cur_depth))


def batch_db_store(dump_batch, c, conn, item, logger, batch_size, closing=False):

    if not closing:
        response_url, url, body = item['response_url'], item['url'], item['body']
        links = " ".join(item['links'])
        appid, crawlid, time_stamp = item['appid'], item['crawlid'], item['timestamp']
        response_headers = str(item['response_headers'])
        request_headers, attrs = str(item['request_headers']), str(item['attrs'])
        status_msg, status_code = item['status_msg'], str(item['status_code'])
        is_pdf = item['is_pdf']
	cur_depth = item['curdepth']

        logger.debug("Adding " + response_url + " to batch, current batch size is " + str(len(dump_batch)))

        new_item = (response_url, url, body, links, is_pdf, appid, crawlid, time_stamp, response_headers, request_headers, attrs, status_msg, status_code, cur_depth)
        dump_batch.append(new_item)
    if len(dump_batch) == batch_size or closing:
        c.executemany("INSERT INTO dump VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", dump_batch)
        conn.commit()
        logger.info("Inserted batch")
	dump_batch = []
    else:
	logger.debug("dump_batch size = " + str(len(dump_batch)) + " and batch_size = " + str(batch_size) + " and closing = " + str(closing))
    return dump_batch

def main():
    # initial main parser setup
    parser = argparse.ArgumentParser(
        description='Kafka Dump: Scrapy Cluster Kafka topic dump utility for '
                    'debugging.', add_help=False)
    parser.add_argument('-h', '--help', action=ArgparseHelper,
                        help='show this help message and exit')

    subparsers = parser.add_subparsers(help='commands', dest='command')

    # args to use for all commands
    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument('-kh', '--kafka-host', action='store', required=False,
                        help="The override Kafka host")
    base_parser.add_argument('-s', '--settings', action='store', required=False,
                        help="The settings file to read from",
                        default="localsettings.py")
    base_parser.add_argument('-ll', '--log-level', action='store', required=False,
                        help="The log level", default=None,
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])

    # list command
    list_parser = subparsers.add_parser('list', help='List all Kafka topics',
                                        parents=[base_parser])

    # dump command
    dump_parser = subparsers.add_parser('dump', help='Dump a Kafka topic',
                                        parents=[base_parser])
    dump_parser.add_argument('-t', '--topic', action='store', required=True,
                             help="The Kafka topic to read from")
    dump_parser.add_argument('-c', '--consumer', action='store',
                             required=False, default=None,
                             help="The Kafka consumer id to use")
    dump_parser.add_argument('-b', '--from-beginning', action='store_const',
                             required=False, const=True,
                             help="Read the topic from the beginning")
    dump_parser.add_argument('-nb', '--no-body', action='store_const',
                             required=False, const=True, default=False,
                             help="Do not include the raw html 'body' key in"
                             " the json dump of the topic")
    dump_parser.add_argument('-p', '--pretty', action='store_const',
                             required=False, const=True, default=False,
                             help="Pretty print the json objects consumed")
    dump_parser.add_argument('-d', '--decode-base64', action='store_const',
                             required=False, const=True, default=False,
                             help="Decode the base64 encoded raw html body")
    dump_parser.add_argument('-sh', '--silent', action='store_const',
                             required=False, const=True, default=False,
                             help="Do not print output to the console")

    args = vars(parser.parse_args())

    wrapper = SettingsWrapper()
    settings = wrapper.load(args['settings'])

    kafka_host = args['kafka_host'] if args['kafka_host'] else settings['KAFKA_HOSTS']
    log_level = args['log_level'] if args['log_level'] else settings['LOG_LEVEL']
    logger = LogFactory.get_instance(level=log_level, name='kafkadump')

    conn = sqlite3.connect(settings['DUMP_PATH'])
    c = conn.cursor()
    logger.debug("Connected to sqlite output db located at {0}".format(settings['DUMP_PATH']))

    if len(list(c.execute("SELECT name FROM sqlite_master WHERE type='table';"))) == 0:
        logger.info("Creating dump table")
        c.execute("CREATE TABLE dump (response_url text, url text, body text, links text, is_pdf text, " +
            "appid text, crawlid text, time_stamp text, response_headers text, request_headers text, " +
            "attrs text, status_msg text, status_code text, curdepth text)")
        logger.info("Created dump table")
        conn.commit()
    else:
        logger.info("Table already exists")

    if args['command'] == 'list':
        try:
            logger.debug("Connecting to {0}...".format(kafka_host))
            kafka = KafkaClient(kafka_host)
            logger.info("Connected to {0}".format(kafka_host))
        except KafkaUnavailableError as ex:
            message = "An exception '{0}' occured. Arguments:\n{1!r}" \
                .format(type(ex).__name__, ex.args)
            logger.error(message)
            sys.exit(1)
        logger.debug('Running list command')
        print("Topics:")
        for topic in list(kafka.topic_partitions.keys()):
            print("-", topic)
        kafka.close()
        return 0
    elif args['command'] == 'dump':
        logger.debug('Running dump command')
        topic = args["topic"]
        consumer_id = args["consumer"]

        try:
            logger.debug("Getting Kafka consumer")

            offset = 'earliest' if args["from_beginning"] else 'latest'

            consumer = KafkaConsumer(
                topic,
                group_id=consumer_id,
                bootstrap_servers=kafka_host,
                consumer_timeout_ms=settings['KAFKA_CONSUMER_TIMEOUT'],
                auto_offset_reset=offset,
                auto_commit_interval_ms=settings['KAFKA_CONSUMER_COMMIT_INTERVAL_MS'],
                enable_auto_commit=settings['KAFKA_CONSUMER_AUTO_COMMIT_ENABLE'],
                max_partition_fetch_bytes=settings['KAFKA_CONSUMER_FETCH_MESSAGE_MAX_BYTES'])
        except NoBrokersAvailable as ex:
                logger.error('Unable to connect to Kafka')
                sys.exit(1)

        num_records = 0
        total_bytes = 0
        item = None

	dump_batch = []
	
        while True:
            try:
                for message in consumer:
                    if message is None:
                        logger.debug("no message")
                        break
                    logger.debug("Received message")
                    val = message.value
                    try:
                        item = json.loads(val)
                        if args['decode_base64'] and 'body' in item:
                            item['body'] = base64.b64decode(item['body'])

                        if args['no_body'] and 'body' in item:
                            del item['body']
                    except ValueError:
                        logger.info("Message is not a JSON object")
                        item = val
                    body_bytes = len(item)
		    if "flush" in item:
			raise Exception

                    dump_batch = batch_db_store(dump_batch, c, conn, item, logger, settings["DB_BATCH_SIZE"])

                    # if args['pretty'] and not args['silent']:
                    #     print(json.dumps(item, indent=4))
                    # else:
                    #     print(item)
                    num_records = num_records + 1
                    total_bytes = total_bytes + body_bytes
            except KeyboardInterrupt:
                logger.debug("Keyboard interrupt received")
                break
            except:
                logger.error(traceback.print_exc())
                break

        total_mbs = old_div(float(total_bytes), (1024*1024))
        if item is not None and not args['silent']:
            print("Last item:")
            print(json.dumps(item, indent=4))
        if num_records > 0:
            logger.info("Num Records: {n}, Total MBs: {m}, kb per message: {kb}"
                    .format(n=num_records, m=total_mbs,
                            kb=(float(total_bytes) / num_records / 1024)))
        else:
            logger.info("No records consumed")
            num_records = 0

        # Commit updates to db, close connection
        if len(dump_batch) != 0:
            batch_db_store(dump_batch, c, conn, None, logger, settings["DB_BATCH_SIZE"], closing=True)
		#	print(item)
            logger.info("Inserted remaining " + str(dump_batch) + " entries into db")
        conn.commit()
        conn.close()
        logger.info("Closing database connection")
        logger.info("Closing Kafka connection")
        try:
            consumer.close()
        except:
            # Exception is thrown when group_id is None.
            # See https://github.com/dpkp/kafka-python/issues/619
            pass
        return 0

if __name__ == "__main__":
    sys.exit(main())
