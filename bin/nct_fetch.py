"""
    Retrieve Clinical Trials and Stores it onto a directory
    @author: Praveen Chandar
"""

from ctgov.utility.log import strd_logger
from multiprocessing import Process, Queue
import argparse, sys, math
import urllib2, urllib3, json
import os, shutil
import re

log = strd_logger('nct-processer')
# create directory (delete if one with the same name already exists)
def mkdir(dirname, force_create=False):
    try:
        os.makedirs(dirname)
    except OSError:
        if force_create:
            shutil.rmtree(dirname)
            os.makedirs(dirname)
        else:
            pass
    except Exception as e:
        log.error(e)
        return False
    return True


def download_web_data(url):
    try:
        con = urllib3.connection_from_url(url)
        html = con.urlopen('GET', url, retries=500, timeout=10)
        con.close()
        return html.data
    except Exception as e:
        log.error('%s: %s' % (e, url))
        return None


def get_clinical_trials():
    """
    Obtains the latest list of all clinical trials from clinicaltrails.gov

    :return:
    """
    url = 'http://clinicaltrials.gov/ct2/crawl'
    html = download_web_data(url)
    pages = re.findall(r'href="/ct2/crawl/(\d+)"', html)
    lnct = set()
    for p in pages:
        html = download_web_data('%s/%s' % (url, p))
        ct = re.findall(r'href="/ct2/show/(NCT\d+)"', html)
        lnct |= set(ct)
    return sorted(lnct)


def get_ct_rawdata(nctid, data_path):
    """
    Downloads the specified trail from clinicaltrial.gov and
        stores the XML File in the specified location.

    :param nctid:
    :param data_path:
    :return:
    """
    url = 'http://clinicaltrials.gov/show/%s?displayxml=true' % nctid
    raw_data = download_web_data(url)
    out_path = data_path + '/' + nctid + '.xml'
    try:
        fid = open(out_path, 'w')
        fid.write(raw_data)
        fid.close()
        return True
    except Exception as e:
        log.error(e)
        return False


def nct_get_ids(dout):
    nct = get_clinical_trials()
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
        mkdir(trials_dout, True)
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

        if not get_ct_rawdata(nctid, data_path):
            retries = 3
            while (retries > 0) and (not get_ct_rawdata(nctid, data_path)):
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
    if not mkdir(args.dout):
        log.error('impossible to create the output directory - interrupting')
        sys.exit(0)
    log.info('output directory: %s \n' % args.dout)

    nct_fetch_dataset(args.dout, args.c)
    log.info('task completed\n')