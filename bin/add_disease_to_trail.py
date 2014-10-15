'''
 	Retrieve Disease - NCT associations starting from a list of diseases

 	@author: Riccardo Miotto
'''

from ctgov.utility.log import strd_logger
from ctgov.utility.web import download_web_data
from collections import defaultdict
import ctgov.index.es_index as es_index
import xml.etree.ElementTree as xml_parser
import ctgov.utility.file as ufile
import argparse, sys

log = strd_logger('disease-nct-association')


def mine_disease_to_nct(ldisease, fout=None, ctmin=100):
    url = 'http://clinicaltrials.gov/search?cond=%s&displayxml=true&count=%s'
    log.info('found %d disease to process \n' % len(ldisease))
    ldisease = sorted(map(lambda x: ' '.join(x.lower().split()), ldisease))
    nct_disease = defaultdict(list)
    c = 1
    for d in sorted(ldisease):
        log.info('processing: "%s"' % d)
        d = d.replace(',', '')
        fd = d.replace(' ', '+')

        # number of trials
        xmltree = xml_parser.fromstring(download_web_data(url % (fd, '0')))
        nres = xmltree.get('count')
        try:
            if int(nres) < ctmin:
                log.info(' --- found only %s trials - skipping \n' % nres)
                continue
        except Exception as e:
            log.error(e)
            continue

        # list of trials
        xmltree = xml_parser.fromstring(download_web_data(url % (fd, nres)))
        lnct = xmltree.findall('clinical_study')
        # nct = set()
        for ct in lnct:
            try:
                cod = ct.find('nct_id')
                # nct.add(cod.text)
                nct_disease[cod.text].append(d)
            except Exception as e:
                log.error(e)
    return nct_disease


def update_ES_index(nct_disease, index_name, host, port_no):
    index = es_index.ElasticSearch_Index(index_name, host=host, port=port_no)

    # Iterate over NCT trials
    for nctid in nct_disease.keys():
        # Get document from the Elastic Search Index
        doc = index.get_trail(nctid)

        if doc is False or not doc.has_key('ec_raw_text'):
            continue

        ec = doc['ec_raw_text']

        if ec is None:
            continue

        doc['disease'] = nct_disease[nctid]
        index.index_trial(nctid, doc)


'''
	main function
'''

# processing the command line options
def _process_args():
    parser = argparse.ArgumentParser(description='Download the Disease - NCT Association')
    # file with the list of disease
    parser.add_argument(dest='fdis', help='list of disease file')

    # index name
    parser.add_argument('-index_name', default='ctgov', help='name of the elastic search index')
    # host name
    parser.add_argument('-host', default='localhost', help='Elastic search hostname')
    # port number
    parser.add_argument('-port', default='9200', help='Elastic search port number')

    # output file
    parser.add_argument('-o', default=None, help='output file (default: None')
    # minimum number of trial allowed
    parser.add_argument('-m', default=100, type=int, help='minimum number of trial allowed (default: 100)')
    return parser.parse_args(sys.argv[1:])


if __name__ == '__main__':
    args = _process_args()

    ldisease = ufile.read_file(args.fdis)
    if ldisease is not None:
        nct_disease = mine_disease_to_nct(ldisease, args.o, args.m)
        update_ES_index(nct_disease, args.index_name, args.host, int(args.port))
    else:
        log.error('no disease found - interrupting')

    log.info('task completed \n')
