"""
 Function to Interact with ClinicalTrials.gov

 @author: Riccardo Miotto <rm3086 (at) columbia (dot) edu>

 Modified on Sep 15th 2014
 @author: Praveen Chandar < (at) columbia (dot) edu >
"""
import re
from ctgov.utility.web import download_web_data
from ctgov.utility.log import strd_logger


log = strd_logger('ctgov-fetch')


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