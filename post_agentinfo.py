#! /usr/bin/env python
import sys
import os
import json
import logging

from logging.handlers import RotatingFileHandler
from argparse import ArgumentParser
from WMAMonElasticInterface import WMAMonElasticInterface

CERT_FILE='/home/stiegerb/.globus/usercert.pem'
KEY_FILE='/home/stiegerb/.globus/plainkey.pem'
# KEY_FILE='/home/stiegerb/.globus/userkey.pem'

def load_data_local(filename='agentinfo4.json'):
    try:
        with open(filename, 'r') as ifile:
            return json.load(ifile)
    except Exception, msg:
        logging.error('Error loading local file: %s' % str(msg))
        return None

def load_data_from_cmsweb():
    from httplib import HTTPSConnection
    con = HTTPSConnection("cmsweb.cern.ch", cert_file=CERT_FILE, key_file=KEY_FILE)
    urn = "/couchdb/wmstats/_design/WMStatsErl/_view/agentInfo"
    headers = {"Content-type": "application/json", "Accept": "application/json"}

    try:
        con.request("GET", urn, headers=headers)
        return json.load(con.getresponse())
    except Exception, msg:
        logging.error('Error connecting to CMSWeb: %s' % str(msg))
        return None

def process_data(raw_data):
    try:
        return [r['value'] for r in raw_data['rows']]
    except Exception, msg:
        logging.error('Error processing data: %s' % str(msg))
        return None

def set_up_logging(args):
    """Configure root logger with rotating file handler"""
    logger = logging.getLogger()

    log_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(log_level, int):
        raise ValueError('Invalid log level: %s' % log_level)
    logger.setLevel(log_level)

    if not os.path.isdir(args.log_dir):
        os.system('mkdir -p %s' % args.log_dir)

    log_file = os.path.join(args.log_dir, 'WMAMonitoring.log')
    filehandler = RotatingFileHandler(log_file, maxBytes=100000)
    filehandler.setFormatter(
        logging.Formatter('%(asctime)s : %(name)s:%(levelname)s - %(message)s'))

    logger.addHandler(filehandler)

def main(args):
    # raw_data = load_data_local()
    raw_data = load_data_from_cmsweb()
    processed_data = process_data(raw_data)
    if not processed_data: return -1

    es_interface = WMAMonElasticInterface(hosts=['localhost:9200'],
                                          index_name='wmamon-dummy',
                                          recreate=args.recreate_index)
    if not es_interface.connected: return -2

    res = es_interface.bulk_inject_from_list_checked(processed_data)
    return 0

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--recreate", action='store_true',
                        dest="recreate_index",
                        help="Recreate the index")
    parser.add_argument("-i", "--index_prefix", default="wmamon-dummy",
                        type=str, dest="index_prefix",
                        help="Index prefix to use [default: %(default)s")
    parser.add_argument("--log_dir", default='log/',
                        type=str, dest="log_dir",
                        help="Directory for logging information [default: %(default)s")
    parser.add_argument("--log_level", default='WARNING',
                        type=str, dest="log_level",
                        help="Log level (CRITICAL/ERROR/WARNING/INFO/DEBUG) [default: %(default)s")
    args = parser.parse_args()
    set_up_logging(args)

    sys.exit(main(args))
