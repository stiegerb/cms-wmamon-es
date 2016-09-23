#! /usr/bin/env python
import sys
import os
import json

from argparse import ArgumentParser

from WMAMonElasticInterface import WMAMonElasticInterface

CERT_FILE='/home/stiegerb/.globus/usercert.pem'
KEY_FILE='/home/stiegerb/.globus/userkey.pem'

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
	res = con.getresponse()
	_data = json.load(res)

	## FIXME: Replace this with requests module
	# import requests
	# Something like this:
	# res = requests.get("https://cmsweb.cern.ch/couchdb/wmstats/_design/WMStatsErl/_view/agentInfo", cert=(CERT_FILE, KEY_FILE))
	# _data = res.json()
	# SSLError ?

_processed_data = None
def process_data():
	global _processed_data
	if not _data:
		load_data_from_cmsweb()
		# load_data()

	_processed_data = [r['value'] for r in _data['rows']]


def main(args):
    es_interface = WMAMonElasticInterface(index_name='wmamon-dummy', recreate=args.recreate_index)

    print 30*'-'
    process_data()

    # es_interface.bulk_inject_from_list(_processed_data)
    res = es_interface.bulk_inject_from_list_checked(_processed_data)
    # es_interface.inject_single_checked(_processed_data[0])

    # return 0

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--recreate", action='store_true',
                        dest="recreate_index",
                        help="Recreate the index")
    parser.add_argument("-i", "--index_prefix", default="wmamon-dummy",
                        type=str, dest="index_prefix",
                        help="Index prefix to use [default: %(default)s")
    args = parser.parse_args()
    main(args)
    # sys.exit(main(args))
