#! /usr/bin/env python
import os
import json
import time
import logging

from logging.handlers import RotatingFileHandler

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from elasticsearch.exceptions import ConnectionError
from elasticsearch.exceptions import ConnectionTimeout

def replace_id(doc):
    """Elastic doesn't like _id fields. Rename field name to _id_prev"""
    _id = doc.pop('_id', None)
    if _id:
        doc['_id_prev'] = _id
    return doc

def helpers_bulk_syntax(doc, index_name, type_name, action='index'):
    """See: http://elasticsearch-py.readthedocs.org/en/
            master/helpers.html#elasticsearch.helpers.bulk"""
    action = {
        '_op_type' : action, # 'index', 'update', 'create', 'delete'
        '_index'   : index_name, # index name
        '_type'    : type_name,
        '_source'  : replace_id(doc) # the actual document
    }
    return action

def wma_mapping(doc_type="agent_info"):
    mapping = {
        "mappings" : {
            doc_type : {
                "properties" : {
                    "timestamp" : {
                        "type"   : "date",
                        "format" : "epoch_second"
                    },
                    "agent_url" : {
                        "type" : "string",
                        "fields" : {
                            "raw" : {
                                "type" : "string",
                                "index" : "not_analyzed"
                            }
                        }
                    }
                }
            }
        }
    }
    return mapping

class WMAMonElasticInterface(object):
    """docstring for WMAMonElasticInterface"""
    def __init__(self,
                 doc_type='agent_info',
                 index_name='wmamon',
                 log_dir=None,
                 log_level=logging.INFO,
                 recreate=False,
                 hosts=None):
        self.doc_type = doc_type
        self.index_name = None
        self.logger = self.set_up_logger(log_dir, log_level=log_level)

        self.es_handle = Elasticsearch(hosts=hosts)
        if not self.check_connection():
            return

        self.make_index(index_name, recreate=recreate,
                        mappings=json.dumps(wma_mapping(doc_type=self.doc_type)))

    def check_connection(self):
        try:
            self.es_handle.ping()
            self.connected = True
            return True
        except ConnectionError:
            self.logger.critical("Elasticsearch connection failed")
            self.connected = False
            return False

    def set_up_logger(self, log_dir, log_level=logging.DEBUG):
        if not log_dir:
            log_dir = os.path.dirname(__file__)
        if not os.path.isdir(log_dir):
            os.system('mkdir -p %s' % log_dir)

        log_file = os.path.join(log_dir, 'WMA_monitoring.log')
        logger = logging.getLogger('WMA_monitoring')
        logger.setLevel(log_level)
        filehandler = RotatingFileHandler(log_file, maxBytes=100000)
        filehandler.setFormatter(
            logging.Formatter('%(asctime)s : %(name)s:%(levelname)s - %(message)s'))
        logger.addHandler(filehandler)

        return logger

    def make_index(self, name, recreate=False, mappings=None):
        """Create the index and set mappings and settings"""
        if self.index_name is None or recreate:
            self.index_name = name

            # create the index
            res = self.es_handle.indices.create(index=self.index_name,
                                                body=mappings, ## FIXME: What happens with 'None'?
                                                ignore=400)
            if res.get("status") != 400:
                self.logger.debug("Created index %s" % (self.index_name))
            elif res['error']['root_cause'][0]['reason'] == 'already exists':
                self.logger.debug("Using existing index %s" % (self.index_name))
            else:
                self.logger.error("Error when creating index: %s" % str(res['error']))

        return self.index_name

    def bulk_inject_from_list(self, docs):
        self.logger.debug("Injecting from list with %d documents" % len(docs))

        actions = (helpers_bulk_syntax(d, index_name=self.index_name, type_name=self.doc_type) for d in docs)

        start_time = time.time()

        res = helpers.bulk(self.es_handle, actions, chunk_size=1000,
                           raise_on_error=False,
                           raise_on_exception=False)

        elapsed = time.time()-start_time

        if not res[0]:
            self.logger.error("Failed to inject %d docs, printing error message" % len(docs))
            try:
                self.logger.error(res[1][0].get('index').get('error'))
            except IndexError:
                self.logger.error(repr(res))
        else:
            self.logger.info("Injected %d docs to %s in %.1f seconds" % (res[0], self.index_name, elapsed))


        return res

    def bulk_inject_from_list_checked(self, docs):
        checked_docs = [d for d in docs if not self.check_if_exists(d['timestamp'], d['agent_url'])]
        self.logger.debug("Found %d new docs" % len(checked_docs))
        if not len(checked_docs): return None
        return self.bulk_inject_from_list(checked_docs)

    def inject_single(self, doc):
        doc = replace_id(doc)
        res = self.es_handle.index(index=self.index_name, doc_type=self.doc_type, body=doc)
        if not res[0]:
            self.logger.error("Failed to inject doc, printing error message")
            try:
                self.logger.error(res[1][0].get('index').get('error'))
            except IndexError:
                self.logger.error(repr(res))
        else:
            self.logger.info("Injected one doc to %s" % (self.index_name))
        return res

    def inject_single_checked(self, doc):
        if not self.check_if_exists(doc['timestamp'], doc['agent_url']):
            return self.inject_single(doc)
        return None

    def check_if_exists(self, timestamp, agent_url):
        query = {
                    "query": {
                        "bool": {
                            "must": [
                                { "match" : { "timestamp": str(timestamp) } },
                                { "match" : { "agent_url": str(agent_url) } }
                            ]
                        }
                    },
                    "size" : 1,
                    "_source" : ["timestamp", "agent_url"]
                }
        try:
            res = self.es_handle.search(body=json.dumps(query), index=self.index_name, timeout='5s')
        except ConnectionTimeout as e:
            self.logger.error(repr(e))
        return res['hits']['total'] > 0

