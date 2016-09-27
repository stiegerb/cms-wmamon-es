#! /usr/bin/env python
import sys
import os
import json

from argparse import ArgumentParser

from WMAMonElasticInterface import WMAMonElasticInterface

CERT_FILE='/home/stiegerb/.globus/usercert.pem'
KEY_FILE='/home/stiegerb/.globus/plainkey.pem'
# KEY_FILE='/home/stiegerb/.globus/userkey.pem'

_data = None
def load_data_local(filename='agentinfo.json'):
	global _data
	with open(filename, 'r') as ifile:
		data = json.load(ifile)

	_data = data

def load_data_from_cmsweb():
	global _data
	from httplib import HTTPSConnection
	con = HTTPSConnection("cmsweb.cern.ch", cert_file=CERT_FILE, key_file=KEY_FILE)
	urn = "/couchdb/wmstats/_design/WMStatsErl/_view/agentInfo"
	headers = {"Content-type": "application/json", "Accept": "application/json"}
	con.request("GET", urn, headers=headers)
	_data = json.load(con.getresponse())

	# ## TODO: Replace this with requests module
	# import requests
	# url = 'https://cmsweb.cern.ch/couchdb/wmstats/_design/WMStatsErl/_view/agentInfo'
	# res = requests.get(url, cert=(CERT_FILE, KEY_FILE), verify=CERT_FILE) # FIXME fails certificate verification
	# _data = res.json()

_processed_data = None
def process_data():
	global _processed_data
	if not _data:
		load_data_from_cmsweb()
		# load_data()

	_processed_data = [r['value'] for r in _data['rows']]


def main(args):
    es_interface = WMAMonElasticInterface(hosts=['localhost:9200'],
    									  index_name='wmamon-dummy',
    	                                  recreate=args.recreate_index,
    	                                  log_level=10, # DEBUG
    	                                  log_dir='/home/stiegerb/wmamon_es/log')

    process_data()

    res = es_interface.bulk_inject_from_list_checked(_processed_data)
    # es_interface.inject_single_checked(_processed_data[0])

    return 0

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--recreate", action='store_true',
                        dest="recreate_index",
                        help="Recreate the index")
    parser.add_argument("-i", "--index_prefix", default="wmamon-dummy",
                        type=str, dest="index_prefix",
                        help="Index prefix to use [default: %(default)s")
    args = parser.parse_args()
    sys.exit(main(args))
