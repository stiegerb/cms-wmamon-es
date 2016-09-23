#! /usr/bin/env python
import json
import time
from pprint import pprint

from elasticsearch import Elasticsearch
from elasticsearch import helpers

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

class WMAMonElasticInterface(object):
    """docstring for WMAMonElasticInterface"""
    def __init__(self, doc_type='agent_info', index_name='wmamon', recreate=False):
        self.doc_type = doc_type
        self.es_handle = Elasticsearch() ## FIXME: Add url here

        self.index_name = None
        self.make_index(index_name, recreate=recreate, mappings=self.create_mappings())

    def create_mappings(self):
        # map the timestamp field to be recognized as a date
        mapping = {
            "mappings" : {
                self.doc_type : {
                    "properties" : {
                        "timestamp" : {
                            "type"   : "date",
                            "format" : "epoch_second"
                        }
                    }
                }
            }
        }
        return json.dumps(mapping)

    def make_index(self, name, recreate=False, mappings=None):
        """Create the index and set mappings and settings"""
        if self.index_name is None or recreate:
            self.index_name = name

            # create the index
            res = self.es_handle.indices.create(index=self.index_name,
                                                body=mappings, ## FIXME: What happens with 'None'?
                                                ignore=400)
            if res.get("status") != 400:
                print "Created index %s" % (self.index_name)
            elif res['error']['root_cause'][0]['reason'] == 'already exists':
                print "Using existing index %s" % (self.index_name)
            else:
                print "Error when creating index: %s" % str(res['error'])

        return self.index_name

    def bulk_inject_from_list(self, docs):
        try:
            print "Injecting from list with %d documents" % len(docs)
        except TypeError: # no len(docs) possible
            print "Injecting from generator object"

        actions = (helpers_bulk_syntax(d, index_name=self.index_name, type_name=self.doc_type) for d in docs)

        start_time = time.time()

        res = helpers.bulk(self.es_handle, actions, chunk_size=1000,
                           raise_on_error=False,
                           raise_on_exception=False)

        elapsed = time.time()-start_time

        print ("injected %d docs to elastic in %f seconds (%.2f docs/s)" % (
                 res[0], elapsed, res[0]/elapsed))
        if not res[0]:
            print "%s >> Failed to inject docs, printing results" % __name__
            pprint(res)

        return res

    def bulk_inject_from_list_checked(self, docs):
        checked_docs = [d for d in docs if not self.check_if_exists(d['timestamp'], d['agent_url'])]
        print "Found %d new docs" % len(checked_docs)
        return self.bulk_inject_from_list(checked_docs)

    def inject_single(self, doc):
        doc = replace_id(doc)
        res = self.es_handle.index(index=self.index_name, doc_type=self.doc_type, body=doc)
        if not res[0]:
            print "%s >> Failed to inject docs, printing results" % __name__
            pprint(res)
        return res

    def inject_single_checked(self, doc):
        if not self.check_if_exists(doc['timestamp'], doc['agent_url']):
            return self.es_handle.index(index=self.index_name, doc_type=self.doc_type, body=doc)
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
        res = self.es_handle.search(body=json.dumps(query), index=self.index_name)
        return res['hits']['total'] > 0

