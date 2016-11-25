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

_doc_cache = None # agent_url -> last timestamp to be processed
_doc_cache_filename = None
def load_cache(filename='.last_processed.json'):
    global _doc_cache, _doc_cache_filename
    _doc_cache_filename = filename
    if not _doc_cache:
        try:
            with open(filename, 'r') as cfile:
                logging.debug("Loading cache file")
                _doc_cache = json.load(cfile)
        except IOError: # File doesn't exist (yet)
            logging.debug("Cache file not found")
            _doc_cache = {}

    return True

def check_timestamp_in_cache(doc):
    """
    Check if the current doc has a timestamp greater
    than the last one to be indexed for this agent_url

    Always returns True if that agent is not already in the cache
    """
    if not _doc_cache: load_cache()
    return doc['timestamp'] > _doc_cache.get(doc['agent_url'], 0)

def update_cache(docs):
    """
    Update the cache file with the timestamps from these docs
    """
    if not _doc_cache: load_cache()
    _doc_cache.update({(d['agent_url'],d['timestamp']) for d in docs})

    with open(_doc_cache_filename, 'w') as cfile:
        logging.debug("Updating cache file with %d entries" % len(docs))
        json.dump(_doc_cache, cfile, indent=2)


def main(args):
    if args.local_file:
        raw_data = load_data_local(args.local_file)
    else:
        raw_data = load_data_from_cmsweb()

    processed_data = process_data(raw_data)
    if not processed_data: return -1

    es_interface = WMAMonElasticInterface(hosts=['localhost:9200'],
                                          index_name='wmamon-dummy',
                                          recreate=args.recreate_index)
    if not es_interface.connected: return -2

    processed_data = [d for d in processed_data if check_timestamp_in_cache(d)]

    # res = es_interface.bulk_inject_from_list_checked(processed_data)
    res = es_interface.bulk_inject_from_list(processed_data)

    if res[0] == len(processed_data):
        update_cache(processed_data)

    return 0

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--local_file", dest='local_file', default='',
                        help="Inject this local file")
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
