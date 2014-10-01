"""
    <Module Explanation>
    @author: Praveen Chandar
"""
from ctgov.utility.log import strd_logger
import ctgov.index.es_index as es_index
from ctgov.load_data import load_data
import argparse, sys
from py2neo import neo4j, node, rel

log = strd_logger('nct-indexer')


def get_disease_cde_count(index, d, c):
    from elasticsearch_dsl import Q

    q = Q('bool', should=[Q('term', ec_tags_umls=c),
                          Q('term', conditions=d)],
          minimum_should_match=2)
    qt = q.to_dict()
    qdict = dict()
    qdict['query'] = qt
    return index.es.count(index.index_name, index.doc_type, body=qdict)['count']


def get_semantic_types(cde, umls):
    stype = []
    if (umls is not None) and (cde in umls.semantic):
        stype = sorted(umls.semantic[cde] & umls.stype)
    # ecde.append([t[0].replace('/', '-'), t[1], ' - '.join(stype)])
    return stype


def build_graph(ec_index_name, host, port, neo_url, umls):
    # Initialize Graph
    graph_db = neo4j.GraphDatabaseService(neo_url)

    # Create CDE index
    cde_index = graph_db.get_or_create_index(neo4j.Node, 'cde-index')
    disease_index = graph_db.get_or_create_index(neo4j.Node, 'disease-index')

    # Initialize ElasticSearch Index
    index = es_index.ElasticSearch_Index(ec_index_name, host, port)


    # Get all unique conditions
    conditions = index.get_unique_terms('conditions', min_docs=2)
    log.info(' Found %d conditions' % len(conditions))

    # Add Diseases (conditions) with properties to GraphDB
    batch = neo4j.WriteBatch(graph_db)
    for disease, freq in conditions.iteritems():
        tmp = batch.create(node(name=disease, trail_with_disease=freq))
        batch.set_labels(tmp, 'Disease')
        batch.add_to_index(neo4j.Node, disease_index, 'name', disease, tmp)
    batch.run()
    log.info(' Successfully added %d conditions' % len(conditions))

    # Get all unique CDEs
    # cdes = index.get_unique_terms('ec_tags_umls', min_docs=3)
    #print 'cdes = ', len(cdes)

    cdes = index.get_unique_terms('ec_tags_umls', min_docs=5)
    log.info(' Found %d CDEs' % len(cdes))

    # Add CDEs with properties to GraphDB
    batch = neo4j.WriteBatch(graph_db)
    for cde, freq in cdes.iteritems():
        stype = get_semantic_types(cde, umls)
        tmp = batch.create(node(name=cde, num_trail=freq, semantic_types=stype))
        batch.set_labels(tmp, 'CDE')
        batch.add_to_index(neo4j.Node, cde_index, 'name', cde, tmp)
    batch.run()
    log.info('Successfully added %d CDEs' % len(cdes))


    # For each disease-CDE pair get counts
    rel_count = 0
    i = 0
    for cde in cdes.keys():
        i += 1
        batch = neo4j.WriteBatch(graph_db)
        cde_node = graph_db.get_indexed_node('cde-index', 'name', cde)
        for disease in conditions.keys():
            freq = get_disease_cde_count(index, disease, cde)
            if freq <= 2:
                continue
            rel_count += 1
            disease_node = graph_db.get_indexed_node('disease-index', 'name', disease)
            batch.create(rel(cde_node, 'related_to', disease_node, freq=freq))
        if i % 50 == 0:
            log.info('Rel count at %d -- %d out of %d CDEs seen' % (rel_count, i, len(cdes)))
        batch.run()
    log.info('Successfully added %d relationships' % rel_count)


# Main Function

# processing the command line options
def _process_args():
    parser = argparse.ArgumentParser(description='Download and Process Clinical Trials')

    # index name
    parser.add_argument('-index_name', default='ctgov', help='name of the elastic search index')

    # host name
    parser.add_argument('-host', default='localhost', help='Elastic search hostname')

    # port number
    parser.add_argument('-port', default='9200', help='Elastic search port number')

    # neo4j data location name
    parser.add_argument('-neo_url', default='http://localhost:7474/db/data/', help='Neo4j data location')

    # umls directory
    parser.add_argument('-u', default=None, help='umls directory (default: None)')

    # number of processers to use
    parser.add_argument('-c', default=1, type=int, help='number of processors (default: 1)')
    return parser.parse_args(sys.argv[1:])


if __name__ == '__main__':
    args = _process_args()

    edata = load_data(None, args.u, None)
    umls = edata[1]

    build_graph(args.index_name, args.host, args.port, args.neo_url, edata[1])
    log.info('task completed\n')