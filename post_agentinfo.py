#! /usr/bin/env python
import sys
import os
import json
import time
import socket
import logging

from logging.handlers import RotatingFileHandler
from argparse import ArgumentParser
from pprint import pformat

def send_email_alert(recipients, subject, message):
    if not recipients:
        return
    try:
        import smtplib, getpass
        from email.mime.text import MIMEText
        msg = MIMEText(message)
        msg['Subject'] = "%s - %sh: %s" % (socket.gethostname(),
                                          time.strftime("%b %d, %H:%M"),
                                          subject)

        domain = socket.getfqdn()
        if not 'cern.ch' in domain:
            domain = '%s.unl.edu' % socket.gethostname()
        msg['From'] = '%s@%s' % (getpass.getuser(), domain)
        msg['To'] = recipients[0]

        s = smtplib.SMTP('localhost')
        s.sendmail(msg['From'], recipients, msg.as_string())
        s.quit()

    except ImportError:
        logging.warning("Email notification failed: ImportError")
    except Exception, e:
        logging.warning("Email notification failed: %s" % str(e))

def load_data_local(filename='agentinfo.json'):
    try:
        with open(filename, 'r') as ifile:
            return json.load(ifile)
    except Exception as msg:
        logging.error('Error loading local file: %s' % str(msg))
        return None

def load_data_from_cmsweb(args):
    from httplib import HTTPSConnection
    con = HTTPSConnection("cmsweb.cern.ch",
                          cert_file=args.cert_file,
                          key_file=args.key_file)
    urn = "/couchdb/wmstats/_design/WMStatsErl/_view/agentInfo"
    headers = {
                "Content-type": "application/json",
                "Accept": "application/json",
                "User-Agent": "agentInfoCollector"
                }

    try:
        con.request("GET", urn, headers=headers)
        return json.load(con.getresponse())
    except Exception as msg:
        message = 'Error connecting to CMSWeb: %s' % str(msg)
        send_email_alert(args.email_alerts,
                         "post_agentinfo connection failure",
                         message)
        logging.error(message)
        return None

def data_fixup(raw_data):
    """Remove some unwanted key and add some possibly missing keys"""
    for doc in raw_data['rows']:
        ## Add a version number for this script
        doc['value']['version'] = '0.2'

        for keyname in ['_deleted_conflicts', '_id', '_rev', 'acdc']:
            doc['value'].pop(keyname, None)
        try:
            # Ensure we always have 'New', 'Idle', 'Running' fields in
            # WMBS_INFO.activeRunJobByStatus
            for status in ["New", "Idle", "Running"]:
                doc["value"]["WMBS_INFO"].setdefault("activeRunJobByStatus", {}).setdefault(status, 0)
        except KeyError:
            pass  # only agents have the WMBS_INFO key, not central services

def process_site_information(raw_data):
    site_docs = []
    prio_docs = []
    for doc in raw_data['rows']:
        try:
            sitePendCountByPrio = doc['value']['WMBS_INFO'].pop('sitePendCountByPrio', [])
            thresholds          = doc['value']['WMBS_INFO'].pop('thresholds', {})
            thresholdsGQ2LQ     = doc['value']['WMBS_INFO'].pop('thresholdsGQ2LQ', {})
            possibleJobsPerSite = doc['value']['LocalWQ_INFO'].pop('possibleJobsPerSite', [])
            uniqueJobsPerSite   = doc['value']['LocalWQ_INFO'].pop('uniqueJobsPerSite', [])
        except KeyError as e:
            if doc['value']['agent_url'] == 'global_workqueue':
                possibleJobsPerSite = doc['value'].pop('possibleJobsPerSite', [])
                uniqueJobsPerSite = doc['value'].pop('uniqueJobsPerSite', [])
                # thresholds are not availablein global workqueue, so let's fake it
                for status, items in possibleJobsPerSite.iteritems():
                    for item in items:
                        thresholds.setdefault(item['site_name'], {})
            else:
                logging.debug('Missing key in %s: %s' % (doc['value']['agent_url'], str(e)))
                continue

        for site in sorted(thresholds):
            site_doc = {}
            site_doc['site_name'] = site
            site_doc['type'] = "site_info"
            site_doc['agent_url'] = doc['value']['agent_url']
            site_doc['timestamp'] = doc['value']['timestamp']

            site_doc['thresholds'] = thresholds[site]
            site_doc['state'] = site_doc['thresholds'].pop('state', 'Unknown')
            site_doc['thresholdsGQ2LQ'] = thresholdsGQ2LQ.get(site, 0)
            if site in sitePendCountByPrio:
                for prio, jobs in sitePendCountByPrio[site].iteritems():
                    prio_doc = {}
                    prio_doc['site_name'] = site
                    prio_doc['type'] = "priority_info"
                    prio_doc['agent_url'] = doc['value']['agent_url']
                    prio_doc['timestamp'] = doc['value']['timestamp']
                    prio_doc['priority'] = prio
                    prio_doc['count'] = jobs
                    prio_docs.append(prio_doc)

            site_doc['LocalWQ_INFO'] = {}
            for status in possibleJobsPerSite.keys():
                # very inefficient, it would be better if the agent was providing a nested
                # dictionary key'ed by the site name instead of a list of dicts
                lwq_info = {}
                for item in possibleJobsPerSite[status]:
                    if item['site_name'] == site:
                        lwq_info['possibleJobsPerSite'] = item['Jobs']
                        lwq_info['NumElems'] = item['NumElems']
                for item in uniqueJobsPerSite[status]:
                    if item['site_name'] == site:
                        lwq_info['uniqueJobsPerSite'] = item['Jobs']
                site_doc['LocalWQ_INFO'][status] = lwq_info

            site_docs.append(site_doc)

    return raw_data, site_docs, prio_docs

def process_work_information(raw_data):
    work_docs = []
    for doc in raw_data['rows']:
        try:
            workByStatus = doc['value']['LocalWQ_INFO'].pop('workByStatus', [])
        except KeyError as e:
            if doc['value']['agent_url'] == 'global_workqueue':
                workByStatus = doc['value'].pop('workByStatus', [])
            else:
                logging.debug('Missing key in %s: %s' % (doc['value']['agent_url'], str(e)))
                continue

        for status_info in workByStatus:
            work_doc = {}
            work_doc['type'] = "work_info"
            work_doc['agent_url'] = doc['value']['agent_url']
            work_doc['timestamp'] = doc['value']['timestamp']
            work_doc['status'] = status_info['status']
            work_doc['count']  = status_info['count']
            work_doc['sum']    = status_info['sum']
            work_docs.append(work_doc)

    return work_docs

def process_data(raw_data):
    ## Transform the site-by-site information into separate documents
    raw_data, site_docs, prio_docs = process_site_information(raw_data)

    ## Transform the workByStatus metric into separate documents, one by status by node
    work_docs = process_work_information(raw_data)

    try:
        return [r['value'] for r in raw_data['rows']], site_docs, prio_docs, work_docs
    except Exception as msg:
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

def submit_to_elastic(data, args, index_name='wmamon-dummy', doc_type='agent_info'):
    if args.dry_run:
        logging.warning("Dry-run injection to UNL ES, using index_name %s and doc_type %s", index_name, doc_type)
        logging.debug("Data to be injected is:\n%s", pformat(data))
        return

    from WMAMonElasticInterface import WMAMonElasticInterface
    es_interface = WMAMonElasticInterface(hosts=['localhost:9200'],
                                          index_name=index_name,
                                          doc_type=doc_type,
                                          recreate=args.recreate_index)
    if not es_interface.connected: return -2

    res = es_interface.bulk_inject_from_list_checked(data)
    # res = es_interface.bulk_inject_from_list(data)

def submit_to_cern_amq(data, args, type_='cms_wmagent_info'):
    if args.dry_run:
        logging.warning("Dry-run injection to MONIT IT, using type_ %s", type_)
        logging.debug("Data to be injected is:")
        for doc in data:
            logging.debug("%s", pformat(doc))
        return []

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
        raw_data = load_data_from_cmsweb(args)

    if not raw_data:
        logging.error("Failed to load data; aborting.")
        return 0

    data_fixup(raw_data)

    processed_data, site_data, prio_data, work_data = process_data(raw_data)
    if not processed_data: return -1

    # Submit to local UNL ES instance
    submit_to_elastic(processed_data, index_name='wmamon-dummy', args=args)
    submit_to_elastic(site_data, index_name='wmamon-dummy-sites', doc_type='site_info', args=args)
    submit_to_elastic(prio_data, index_name='wmamon-dummy-priorities', doc_type='priority_info', args=args)
    submit_to_elastic(work_data, index_name='wmamon-dummy-work', doc_type='work_info', args=args)

    # Submit to CERN MONIT
    new_data = [d for d in processed_data if check_timestamp_in_cache(d)]
    if not new_data:
        logging.warning("No new documents found")
        return 0
    sent_data = submit_to_cern_amq(new_data, args=args)
    update_cache([b['payload'] for b in sent_data])
    site_data_sent = submit_to_cern_amq(site_data, args=args, type_='cms_wmagent_info_sites')
    prio_data_sent = submit_to_cern_amq(prio_data, args=args, type_='cms_wmagent_info_priorities')
    work_data_sent = submit_to_cern_amq(work_data, args=args, type_='cms_wmagent_info_work')

    logging.warning("Summary of CERN AMQ injection:")
    logging.warning("  Documents submitted for new data: %d", len(sent_data))
    logging.warning("  Documents submitted for site info: %d", len(site_data_sent))
    logging.warning("  Documents submitted for prio info: %d", len(prio_data_sent))
    logging.warning("  Documents submitted for work info: %d", len(work_data_sent))

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
    parser.add_argument("--dry_run", action='store_true', default=False, dest="dry_run",
                        help="Create all the monitoring information but don't inject anything")
    parser.add_argument("--email_alerts", default=[], action='append',
                        dest="email_alerts",
                        help="Email addresses for alerts [default: none]")
    args = parser.parse_args()
    set_up_logging(args)

    sys.exit(main(args))
