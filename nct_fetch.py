'''
 	Retrieve Clinical Trials and Stores it onto a directory
 	
 	@author: Praveen Chandar
'''

from lib.utility.log import strd_logger
from multiprocessing import Process, Queue
import lib.utility.file as ufile
import lib.ctgov as ctgov
import argparse, sys, math
from elasticsearch import Elasticsearch

log = strd_logger('nct-processer')


def nct_processer(dout, nprocs=1):
    # get the list of clinical trials
    log.info('downloading the list of clinical trials')
    nct = ctgov.get_clinical_trials()
    if len(nct) == 0:
        log.error(' --- not found any clinical trials - interrupting \n')
        return
    log.info(' --- found %d clinical trials \n' % len(nct))

    # process each clinical trial
    log.info('processing clinical trials')
    procs = []
    chunksize = int(math.ceil(len(nct) / float(nprocs)))
    for i in xrange(nprocs):
        p = Process(target=_worker, args=(nct[chunksize * i:chunksize * (i + 1)], dout, (i + 1)))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()


# private functions

# processer worker
def _worker(nct, data_path, npr):
    for i in xrange(1, len(nct) + 1):
        nctid = nct[i - 1]
        if i % 500 == 0:
            log.info(' --- core %d: processed %d documents' % (npr, i))

        if not ctgov.get_ct_rawdata(nctid, data_path):
            retries = 3
            while (retries > 0) and (not ctgov.get_ct_rawdata(nctid, data_path)):
                log.info(' --- retrying: %s' % nctid)
                retries -= 1


'''
	main function
'''

# processing the command line options
def _process_args():
    parser = argparse.ArgumentParser(description='Download and Process Clinical Trials')
    # output directory
    parser.add_argument('-dout', help='output data directory')
    # number of processers to use
    parser.add_argument('-c', default=1, type=int, help='number of processors (default: 1)')
    return parser.parse_args(sys.argv[1:])


if __name__ == '__main__':
    args = _process_args()
    if not ufile.mkdir(args.dout):
        log.error('impossible to create the output directory - interrupting')
        sys.exit(0)
    log.info('output directory: %s \n' % args.dout)

    nct_processer(args.dout, args.c)
    log.info('task completed\n')
		


