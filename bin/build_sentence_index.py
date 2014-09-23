"""
    <Module Explanation>
    @author: Praveen Chandar
"""

from ctgov.load_data import load_data
from ctgov.utility.log import strd_logger
from multiprocessing import Process
import ctgov.index.es_index as es_index
from ctgov.tag_miner.textprocesser import TextProcesser
from ctgov.tag_miner.cvalue import substring_filtering
import argparse
import sys
import math


log = strd_logger('tag-miner')


def nct_tagging(index_name, process_ids, stopwords, umls, pos, nprocs=1):
    # open the clinical trail ids file to process
    nct_ids = []
    for line in open(process_ids, 'rb'):
        nct_ids.append(line.strip())

    # Check if index exists
    index = es_index.ElasticSearch_Index(index_name)

    # Create a new sentence index
    sent_index_name = index_name + '_sent'
    sent_index = es_index.ElasticSearch_Index(sent_index_name)
    sent_index.open_index()
    sent_index.add_field('ec_raw_text')
    sent_index.add_field('ec_tags_umls', term_vector=True)



    # Get clinical
    # process each clinical trial and store to XML file
    log.info('processing clinical trials')
    procs = []
    chunksize = int(math.ceil(len(nct_ids) / float(nprocs)))
    for i in xrange(nprocs):
        p = Process(target=_worker, args=(nct_ids[chunksize * i:chunksize * (i + 1)],
                                          index_name, sent_index_name,
                                          stopwords, umls, pos, (i + 1)))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()


# private functions

# processer worker
# indexer function
def _worker(nct, index_name, sent_index_name, stopwords, umls, pos, npr):
    index = es_index.ElasticSearch_Index(index_name)
    sent_index = es_index.ElasticSearch_Index(sent_index_name)
    for i in xrange(1, len(nct) + 1):
        nctid = nct[i - 1]
        print nctid
        if i % 500 == 0:
            log.info(' --- core %d: indexed %d documents' % (npr, i))

        doc = index.get_trail(nctid)
        ec = doc['ec_raw_text']

        if ec == None:
            continue

        sent_id = 1
        for it in ec:
            sent = ec[it].split(' - ')

            for s in sent:
                tags = {}
                proc = TextProcesser(s, 5, stopwords, umls, pos)
                proc.process()
                for pp in proc.ptxt:
                    freq = tags.setdefault(pp, 0)
                    tags[pp] = freq + 1

                if len(tags) == 0:
                    continue

                dictlist = []
                for key, value in substring_filtering(tags, 1).iteritems():
                    temp = [key, value]
                    dictlist.append(temp)

                # Index the tags
                sent_data = {}
                sent_data['id'] = nctid + '_' + str(sent_id)
                sent_data['ec_raw_text'] = s.strip()
                sent_data['ec_tags_umls'] = dictlist

                sent_index.index_trial(nctid + '_' + str(sent_id), sent_data)

                sent_id += 1


# Main Function

# processing the command line options
def _process_args():
    parser = argparse.ArgumentParser(description='Download and Process Clinical Trials')

    # index name
    parser.add_argument('-index_name', default='ctgov', help='name of the elastic search index')

    # ids file
    parser.add_argument('-process_ids', help='file containing clinical ids to process')

    # stop word file
    parser.add_argument('-w', default=None, help='stop word directory (default: None)')
    # umls directory
    parser.add_argument('-u', default=None, help='umls directory (default: None)')
    # pos tags
    parser.add_argument('-p', default=None, help='part-of-speech admitted tag file (default: None)')

    # number of processers to use
    parser.add_argument('-c', default=1, type=int, help='number of processors (default: 1)')
    return parser.parse_args(sys.argv[1:])


if __name__ == '__main__':
    args = _process_args()

    edata = load_data(args.w, args.u, args.p)
    nct_tagging(args.index_name, args.process_ids, edata[0], edata[1], edata[2], args.c)
    log.info('task completed\n')