"""
    Retrieve Clinical Trials and Stores it onto a directory
    @author: Praveen Chandar
"""

from ctgov.utility.log import strd_logger
from multiprocessing import Process, Queue
import ctgov.utility.file as file_utils
import ctgov.index as ctgov_index
import argparse, sys, math
import os

log = strd_logger('nct-processer')


def nct_get_ids(dout):
    nct = ctgov_index.get_clinical_trials()
    if len(nct) == 0:
        log.error(' --- not found any clinical trials - interrupting \n')
        return
    log.info(' --- found %d clinical trials \n' % len(nct))

    # store the fetched clinical trail ids from clinicaltrail.gov to a file
    nct_id_file = open(dout + '/trial_ids.txt', 'w')
    for nct_id in nct:
        nct_id_file.write(nct_id + '\n')
    nct_id_file.close()
    return nct


def nct_fetch_dataset(dout, nprocs=1):
    # get the list of clinical trials
    log.info('downloading the list of clinical trials')

    # Check directories
    if not os.path.exists(dout):
        log.error('Base directory path is invalid -- %s does not exists\n' % dout)
        exit(0)
    trials_dout = dout + '/trials_xml/'
    if not os.path.exists(trials_dout):
        file_utils.mk_new_dir(trials_dout)
    else:
        log.info('data output directory already exists -- files will be overwritten')

    # Get clinical trail ids
    nct = nct_get_ids(dout)


    # process each clinical trial and store to XML file
    log.info('processing clinical trials')
    procs = []
    chunksize = int(math.ceil(len(nct) / float(nprocs)))
    for i in xrange(nprocs):
        p = Process(target=_worker, args=(nct[chunksize * i:chunksize * (i + 1)], trials_dout, (i + 1)))
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

        if not ctgov_index.get_ct_rawdata(nctid, data_path):
            retries = 3
            while (retries > 0) and (not ctgov_index.get_ct_rawdata(nctid, data_path)):
                log.info(' --- retrying: %s' % nctid)
                retries -= 1


# Main Function

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
    if not file_utils.mkdir(args.dout):
        log.error('impossible to create the output directory - interrupting')
        sys.exit(0)
    log.info('output directory: %s \n' % args.dout)

    nct_fetch_dataset(args.dout, args.c)
    log.info('task completed\n')