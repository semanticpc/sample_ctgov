'''
 Function to Interact with ClinicalTrials.gov

 @author: Riccardo Miotto <rm3086 (at) columbia (dot) edu>

 Modified on Sep 15th 2014
 @author: Praveen Chandar < (at) columbia (dot) edu >

 '''

import re
from lib.utility.web import download_web_data
from lib.utility.log import strd_logger


log = strd_logger('ct-retrieval')

# list of all trials available
def get_clinical_trials():
    url = 'http://clinicaltrials.gov/ct2/crawl'
    html = download_web_data(url)
    pages = re.findall(r'href="/ct2/crawl/(\d+)"', html)
    lnct = set()
    for p in pages:
        html = download_web_data('%s/%s' % (url, p))
        ct = re.findall(r'href="/ct2/show/(NCT\d+)"', html)
        lnct |= set(ct)
    return sorted(lnct)


# Given a nctid -- function retr
def get_ct_rawdata(nctid, data_path):
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