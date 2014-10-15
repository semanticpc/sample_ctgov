"""
    The module contains functions to connect to the elastic search index.

    @author: Praveen Chandar
"""
from ctgov.utility.log import strd_logger
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch import ConnectionError
import json

log = strd_logger('elasticsearch-index')


class ElasticSearch_Index(object):
    def __init__(self, index_name, host='localhost', port=9200):
        self.index_name = index_name
        self.host_name = host
        self.port_number = port
        self.doc_type = 'trial'

        self.es = self.get_es_conn()


    def get_es_conn(self):
        """
        Create an ElasticSearch() object

        :return: Elasticsearch() instance
        """
        assert isinstance(self.host_name, str)

        host = [{'host': self.host_name, 'port': self.port_number}]

        try:
            es = Elasticsearch(host)
            self.search_obj = Search(es)
        except ConnectionError as ce:
            log.error('Unable to connect to Elastic Search Server')
            exit(0)
        return es


    def open_index(self, settings_file=None):
        """
        Opens an index (Creates an index if not present)

        :param es: ElasticSearch instance
        :param index_name: Name of the index to be created in Elastic Search
        :param settings_file: Path to the ES settings files
        :return: Boolean value indication success of the operation
        """
        assert isinstance(self.es, Elasticsearch)

        # Create a new index if it doesn't exist
        # Open the the index
        if self.es.indices.exists(self.index_name):
            self.es.indices.open(self.index_name)
            return True
        else:
            # Read setting from the file
            if settings_file != None:
                settings = open(settings_file, mode='r')
                res = self.es.indices.create(index=self.index_name, body=settings.read())
            else:
                res = self.es.indices.create(index=self.index_name)
                self._init_mapping()
            if res['acknowledged'] == True:
                return True
            else:
                log.info('Unable to create index')
                return False


    def delete_index(self):
        """
        Delete the index with the specified name
        [ALL DATA IN THE INDEX WILL BE LOST}

        :param index_name:
        """
        assert isinstance(self.es, Elasticsearch)
        assert isinstance(self.index_name, str)

        if self.es.indices.exists(self.index_name):
            self.es.indices.delete(self.index_name)
            return True


    def index_trial(self, nct_id, trail_data):
        """

        :param es:
        :param trail_data:
        """

        res = self.es.index(index=self.index_name,
                            doc_type=self.doc_type,
                            id=nct_id,
                            body=trail_data)

        return res['created']

    def get_trail(self, nct_id):
        assert isinstance(self.es, Elasticsearch)
        try:
            out = self.es.get(index=self.index_name, id=nct_id, doc_type=self.doc_type)['_source']
        except Exception as e:
            # log.error('Unable to get trail id %s' % nct_id)
            return False
        return out

    def _init_mapping(self):
        assert isinstance(self.es, Elasticsearch)
        try:


            mapping = {}
            mapping['_id'] = {'store': 'true', 'index': 'not_analyzed', 'path': 'id'}
            mapping['_index'] = {'enabled': 'true'}
            mapping['_type'] = {'store': 'true'}
            mapping['_timestamp'] = {'enabled': 'true', 'store': 'true'}
            mapping['properties'] = {}
            mapping_main = {}
            mapping_main[self.doc_type] = mapping

            self.es.indices.put_mapping(index=self.index_name, doc_type=self.doc_type, body=mapping_main)
        except Exception as e:
            log.error('Unable to put initial mapping to index -- %s ' % e)
            return False
        return True

    def add_field(self, field_name, term_vector=False):
        assert isinstance(self.es, Elasticsearch)
        try:

            field_prop = {}
            field_prop['type'] = 'string'
            field_prop['store'] = 'yes'
            field_prop['index'] = 'not_analyzed'
            if term_vector:
                field_prop['term_vector'] = 'yes'

            mapping = {}
            mapping['properties'] = {}
            mapping['properties'][field_name] = field_prop
            mapping_main = {}
            mapping_main[self.doc_type] = mapping

            self.es.indices.put_mapping(index=self.index_name, doc_type=self.doc_type, body=mapping_main)
        except Exception as e:
            log.error('Unable to put mapping to index -- %s ' % e)
            return False
        return True

    def get_unique_terms(self, field_name, min_docs=5):
        assert isinstance(self.search_obj, Search)

        # define a bucket aggregation and metrics inside:

        self.search_obj.aggs.bucket('tokens', 'terms', field=field_name, size=20)
        s = Search(self.es).index(self.index_name)
        s.query('match_all')
        s.aggs.bucket('myaggs', 'terms', field=field_name, size=0, min_doc_count=min_docs)

        res = {}
        for i in s.execute().aggregations.myaggs.buckets:
            res[i['key']] = i['doc_count']
        return res

    def query_1(self):
        body = {"query": {
            "filtered": {
                "query": {
                    "bool": {
                        "must": [{"term": {"ec_tags_umls": "ecg normal"}},
                                 {"term": {"ec_tags_umls": "liver diseases"}}],


                    }
                }
            }

        }
        }
        res = self.es.search(self.index_name, self.doc_type, body)
        print res['hits']['total']
        for hit in res['hits']['hits']:
            print hit['_id']