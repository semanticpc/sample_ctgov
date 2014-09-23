'''
 	Retrieve Disease - NCT associations starting from a list of diseases

 	@author: Riccardo Miotto
'''

from ctgov.utility.log import strd_logger
from ctgov.utility.web import download_web_data
import xml.etree.ElementTree as xml_parser
import ctgov.utility.file as ufile
import ctgov.index as ctgov
import argparse, sys

log = strd_logger('disease-nct-association')


def mine_disease_to_nct(ldisease, fout=None, ctmin=100):
    url = 'http://clinicaltrials.gov/search?cond=%s&displayxml=true&count=%s'
    disease_to_nct = {}
    stat = []
    log.info('found %d disease to process \n' % len(ldisease))
    ldisease = sorted(map(lambda x: ' '.join(x.lower().split()), ldisease))

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
        nct = set()
        for ct in lnct:
            try:
                cod = ct.find('nct_id')
                nct.add(cod.text)
            except Exception as e:
                log.error(e)

        disease_to_nct[d] = nct
        log.info('--- found %d clinical trials' % len(nct))
        stat.append([d, len(nct)])
        print ''

    # save
    try:
        ufile.write_obj(fout, disease_to_nct)
        log.info('Disease - NCT association correctly saved')
        log.info('--- out: %s' % fout)
    except Exception as e:
        log.error('impossible to save the disease-to-nct association')
        log.error('--- %s' % e)

    try:
        fstat = '%s.csv' % fout[:fout.rfind('.')]
        ufile.write_csv(fstat, stat)
        log.info('--- statistics: %s' % fstat)
    except Exception as e:
        log.error('impossible to save the disease statistics')
        log.error('--- %s' % e)

    return disease_to_nct


'''
	main function
'''

# processing the command line options
def _process_args():
    parser = argparse.ArgumentParser(description='Download the Disease - NCT Association')
    # file with the list of disease
    parser.add_argument(dest='fdis', help='list of disease file')
    # output file
    parser.add_argument('-o', default=None, help='output file (default: None')
    # minimum number of trial allowed
    parser.add_argument('-m', default=100, type=int, help='minimum number of trial allowed (default: 100)')
    return parser.parse_args(sys.argv[1:])


if __name__ == '__main__':
    args = _process_args()
    print ''
    ldisease = ufile.read_file(args.fdis)
    if ldisease is not None:
        mine_disease_to_nct(ldisease, args.o, args.m)
    else:
        log.error('no disease found - interrupting')
    print ''
    log.info('task completed \n')
		
	
		

	