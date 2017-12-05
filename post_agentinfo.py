#! /usr/bin/env python
import sys
import os
import json
import logging
import socket

from logging.handlers import RotatingFileHandler
from argparse import ArgumentParser


def load_data_local(filename='agentinfo.json'):
    try:
        with open(filename, 'r') as ifile:
            return json.load(ifile)
    except Exception, msg:
        logging.error('Error loading local file: %s' % str(msg))
        return None

def load_data_from_cmsweb(cert_file, key_file):
    from httplib import HTTPSConnection
    con = HTTPSConnection("cmsweb.cern.ch", cert_file=cert_file, key_file=key_file)
    urn = "/couchdb/wmstats/_design/WMStatsErl/_view/agentInfo"
    headers = {
                "Content-type": "application/json",
                "Accept": "application/json",
                "User-Agent": "agentInfoCollector"
                }

    try:
        con.request("GET", urn, headers=headers)
        return json.load(con.getresponse())
    except Exception, msg:
        logging.error('Error connecting to CMSWeb: %s' % str(msg))
        return None

def process_site_information(raw_data):
    site_docs = []
    prio_docs = []
    work_docs = []
    for doc in raw_data['rows']:
        try:
            sitePendCountByPrio = doc['value']['WMBS_INFO'].pop('sitePendCountByPrio', [])
            thresholds          = doc['value']['WMBS_INFO'].pop('thresholds', [])
            thresholdsGQ2LQ     = doc['value']['WMBS_INFO'].pop('thresholdsGQ2LQ', [])
            possibleJobsPerSite = doc['value']['LocalWQ_INFO'].pop('possibleJobsPerSite', [])
            uniqueJobsPerSite   = doc['value']['LocalWQ_INFO'].pop('uniqueJobsPerSite', [])
            workByStatus        = doc['value']['LocalWQ_INFO'].pop('workByStatus', [])
        except (KeyError) as e:
            logging.debug('Missing key in %s: %s' % (doc['value']['agent_url'], str(e)))
            continue

        sites = sorted(list(set(thresholds.keys() + 
                                thresholdsGQ2LQ.keys() + 
                                sitePendCountByPrio.keys())))

        for site in sites:
            site_doc = {}
            site_doc['site_name'] = site
            site_doc['type'] = "site_info"
            site_doc['agent_url'] = doc['value']['agent_url']
            site_doc['timestamp'] = doc['value']['timestamp']
            site_doc['WMBS_INFO'] = {}
            if site in thresholds:
                site_doc['WMBS_INFO']['thresholds'] = thresholds[site]
            if site in thresholdsGQ2LQ:
                site_doc['WMBS_INFO']['thresholdsGQ2LQ'] = thresholdsGQ2LQ[site]
            if site in sitePendCountByPrio:
                priocount = []
                for prio, count in sitePendCountByPrio[site].iteritems():
                    prio_doc = {}
                    prio_doc['site_name'] = site
                    prio_doc['type'] = "priority_info"
                    prio_doc['agent_url'] = doc['value']['agent_url']
                    prio_doc['timestamp'] = doc['value']['timestamp']
                    prio_doc['priority'] = prio
                    prio_doc['count'] = count
                    prio_docs.append(prio_doc)

            site_doc['LocalWQ_INFO'] = {}
            for status in possibleJobsPerSite.keys():
                lwq_info = {}
                if site in possibleJobsPerSite[status]:
                    lwq_info['possibleJobsPerSite'] = possibleJobsPerSite[status][site]['Jobs']
                    lwq_info['NumElems'] = possibleJobsPerSite[status][site]['NumElems']
                    lwq_info['uniqueJobsPerSite'] = uniqueJobsPerSite[status][site]['Jobs']
                site_doc['LocalWQ_INFO'][status] = lwq_info

            site_docs.append(site_doc)

        for status_info in workByStatus:
            work_doc = {}
            work_doc['type'] = "work_info"
            work_doc['agent_url'] = doc['value']['agent_url']
            work_doc['timestamp'] = doc['value']['timestamp']
            work_doc['status'] = status_info['status']
            work_doc['count']  = status_info['count']
            work_doc['sum']    = status_info['sum']
            work_docs.append(work_doc)

    return raw_data, site_docs, prio_docs, work_docs

def process_data(raw_data):
    # Ensure we always have 'New', 'Idle', 'Running' fields in 
    # WMBS_INFO.activeRunJobByStatus
    for doc in raw_data['rows']:
        try:
            for status in ["New", "Idle", "Running"]:
                doc["value"]["WMBS_INFO"].setdefault("activeRunJobByStatus", {}).setdefault(status, 0)
        except KeyError: pass # Special agents don't have 'WMBS_INFO' in the first place

    ## Transform the site-by-site information into separate documents
    raw_data, site_docs, prio_docs, work_docs = process_site_information(raw_data)

    ## Add a version number:
    for doc in raw_data['rows']:
        doc['value']['version'] = '0.2'

    try:
        return [r['value'] for r in raw_data['rows']], site_docs, prio_docs, work_docs
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
def load_cache(filename='/home/stiegerb/wmamon_es/.last_processed.json'):
    global _doc_cache, _doc_cache_filename
    _doc_cache_filename = filename
    if not _doc_cache:
        try:
            with open(filename, 'r') as cfile:
                logging.debug("Loading cache file")
                _doc_cache = json.load(cfile)
        except ValueError: # File is empty?!
            logging.debug("Cache file is empty")
            _doc_cache = {}
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

def submit_to_elastic(data, index_name='wmamon-dummy', doc_type='agent_info'):
    from WMAMonElasticInterface import WMAMonElasticInterface
    es_interface = WMAMonElasticInterface(hosts=['localhost:9200'],
                                          index_name=index_name,
                                          doc_type=doc_type,
                                          recreate=args.recreate_index)
    if not es_interface.connected: return -2

    res = es_interface.bulk_inject_from_list_checked(data)
    # res = es_interface.bulk_inject_from_list(data)

def submit_to_cern_amq(data, args, type_='cms_wmagent_info'):
    try:
        import stomp
    except ImportError as e:
        logging.warning("stomp.py not found, skipping submission to CERN/AMQ")
        return []
    from StompAMQ import StompAMQ
    StompAMQ._version = '0.1.2'

    try:
        username = open(args.username, 'r').read().strip()
        password = open(args.password, 'r').read().strip()
    except IOError:
        username = args.username
        password = args.password
    stomp_interface = StompAMQ(username=username,
                               password=password,
                               host_and_ports=[('dashb-mb.cern.ch', 61113)])

    list_data = []
    for doc in data:
        id_ = doc.pop("_id", None)
        list_data.append(stomp_interface.make_notification(payload=doc,
                                                           id_=id_,
                                                           type_=type_))

    sent_data = stomp_interface.send(list_data)
    return sent_data


def main(args):
    if args.local_file:
        raw_data = load_data_local(args.local_file)
    else:
        raw_data = load_data_from_cmsweb(args.cert_file, args.key_file)

    processed_data, site_data, prio_data, work_data = process_data(raw_data)
    if not processed_data: return -1

    # Submit to local UNL ES instance
    submit_to_elastic(processed_data, index_name='wmamon-dummy')
    submit_to_elastic(site_data, index_name='wmamon-dummy-sites', doc_type='site_info')
    submit_to_elastic(prio_data, index_name='wmamon-dummy-priorities', doc_type='priority_info')
    submit_to_elastic(work_data, index_name='wmamon-dummy-work', doc_type='work_info')

    # Submit to CERN MONIT
    new_data = [d for d in processed_data if check_timestamp_in_cache(d)]
    if not new_data:
        logging.warning("No new documents found")
        return 0
    sent_data = submit_to_cern_amq(new_data, args=args)
    update_cache([b['payload'] for b in sent_data])
    submit_to_cern_amq(site_data, args=args, type_='cms_wmagent_info_sites')
    submit_to_cern_amq(prio_data, args=args, type_='cms_wmagent_info_priorities')
    submit_to_cern_amq(work_data, args=args, type_='cms_wmagent_info_work')

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
                        help="Index prefix to use [default: %(default)s]")
    parser.add_argument("--log_dir", default='log/',
                        type=str, dest="log_dir",
                        help="Directory for logging information [default: %(default)s]")
    parser.add_argument("--log_level", default='WARNING',
                        type=str, dest="log_level",
                        help="Log level (CRITICAL/ERROR/WARNING/INFO/DEBUG) [default: %(default)s]")
    parser.add_argument("--cert_file", default=os.getenv('X509_USER_PROXY'),
                        type=str, dest="cert_file",
                        help="Client certificate file [default: %(default)s]")
    parser.add_argument("--key_file", default=os.getenv('X509_USER_PROXY'),
                        type=str, dest="key_file",
                        help="Certificate key file [default: %(default)s]")
    parser.add_argument("--username", default='username',
                        type=str, dest="username",
                        help="Plaintext username or file containing it [default: %(default)s]")
    parser.add_argument("--password", default='password',
                        type=str, dest="password",
                        help="Plaintext password or file containing it [default: %(default)s]")
    args = parser.parse_args()
    set_up_logging(args)

    sys.exit(main(args))
