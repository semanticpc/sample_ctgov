"""
    <Module Explanation>
    @author: Praveen Chandar
"""
from ctgov.utility.log import strd_logger
from multiprocessing import Process, Queue
import ctgov.utility.file as file_utils
import ctgov.index.es_index as es_index
import ctgov.index.ctgov_parser as ctgov_parser
import argparse, sys, math
import os

log = strd_logger('nct-indexer')


def nct_index(din, index_name, settings_file=None, nprocs=1):
    # open the clinical trail ids file and load to a list
    log.info('opening file -- trial_ids.txt')

    nct_ids = []
    for line in open(din + '/trial_ids.txt', 'rb'):
        nct_ids.append(line.strip())


    # Check directories
    trials_din = din + '/trials_xml/'
    if not os.path.exists(trials_din):
        log.error('trials_xml directory does not exists in %s \n' % din)
        exit(0)

    # Create index using the provided settings file
    index = es_index.ElasticSearch_Index(index_name)
    if settings_file == None:
        index.open_index()
    else:
        index.open_index(settings_file)



    # process each clinical trial and store to XML file
    log.info('processing clinical trials')
    procs = []
    chunksize = int(math.ceil(len(nct_ids) / float(nprocs)))
    for i in xrange(nprocs):
        p = Process(target=_worker, args=(nct_ids[chunksize * i:chunksize * (i + 1)],
                                          trials_din, index_name, (i + 1)))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()


# private functions

# processer worker
# indexer function
def _worker(nct, data_path, index_name, npr):
    index = es_index.ElasticSearch_Index(index_name)
    parser = ctgov_parser.ClinicalTrial_Parser(data_path)
    # trail_doc = parser.parse('NCT00000180')
    # print trail_doc
    # index.index_trial('NCT00000180',trail_doc)
    # return
    for i in xrange(1, len(nct) + 1):
        nctid = nct[i - 1]

        if i % 500 == 0:
            log.info(' --- core %d: indexed %d documents' % (npr, i))
        trail_doc = parser.parse(nctid)

        index.index_trial(nctid, trail_doc)

        # Close the index


# Main Function

# processing the command line options
def _process_args():
    parser = argparse.ArgumentParser(description='Download and Process Clinical Trials')

    # index name
    parser.add_argument('-index_name', default='ctgov', help='name of the elastic search index')

    # number of processers to use
    parser.add_argument('-c', default=1, type=int, help='number of processors (default: 1)')
    return parser.parse_args(sys.argv[1:])


if __name__ == '__main__':
    args = _process_args()
    index = es_index.ElasticSearch_Index('test-index')
    index.search('conditions')

    log.info('task completed\n')