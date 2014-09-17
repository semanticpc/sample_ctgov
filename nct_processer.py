'''
 	Retrieve and Process Clinical Trials (extract n-grams from eligibility criteria)
 	
 	@author: Riccardo Miotto
'''

from lib.utility.log import strd_logger
from lib.load_data import load_data
from multiprocessing import Process, Queue
from lib.ctgov.clinicaltrial import ClinicalTrial
import lib.utility.file as ufile
import lib.ctgov as ctgov
import argparse, sys, math

log = strd_logger('nct-processer')


def nct_processer(dout, stop=None, umls=None, ptag=None, nprocs=1):
    # get the list of clinical trials
    log.info('downloading the list of clinical trials')
    nct = ctgov.get_clinical_trials()
    if len(nct) == 0:
        log.error(' --- not found any clinical trials - interrupting \n')
        return
    log.info(' --- found %d clinical trials \n' % len(nct))

    # process each clinical trial
    log.info('processing clinical trials')
    qout = Queue()
    procs = []
    chunksize = int(math.ceil(len(nct) / float(nprocs)))
    for i in xrange(nprocs):
        p = Process(target=_worker, args=(nct[chunksize * i:chunksize * (i + 1)], stop, umls, ptag, qout, (i + 1)))
        procs.append(p)
        p.start()

    pnct = {}
    for i in range(nprocs):
        pdata = qout.get()
        pnct.update(pdata)

    for p in procs:
        p.join()

    # save
    fout = '%s/clinical-trials.pkl' % dout
    if not ufile.write_obj(fout, pnct):
        log.error('error in saving the data - skipping')

    return pnct


# private functions

# processer worker
def _worker(nct, stop, umls, ptag, qout, npr):
    pnct = {}
    for i in xrange(1, len(nct) + 1):
        nctid = nct[i - 1]
        if i % 500 == 0:
            log.info(' --- core %d: processed %d documents' % (npr, i))
        pk = ClinicalTrial(nctid)
        if not pk.retrieve():
            retries = 3
            while (retries > 0) and (not pk.retrieve()):
                log.info(' --- retrying: %s' % pk.id)
                retries -= 1
        pk.ec_process(stop, umls, ptag)
        pnct[nctid] = pk
    qout.put(pnct)


'''
	main function
'''

# processing the command line options
def _process_args():
    parser = argparse.ArgumentParser(description='Download and Process Clinical Trials')
    # output directory
    parser.add_argument(dest='dout', help='output data directory')
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
    if not ufile.mkdir(args.dout):
        log.error('impossible to create the output directory - interrupting')
        sys.exit(0)
    log.info('output directory: %s \n' % args.dout)

    edata = load_data(args.w, args.u, args.p)
    nct_processer(args.dout, edata[0], edata[1], edata[2], args.c)
    log.info('task completed\n')
		


