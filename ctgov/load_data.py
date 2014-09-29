'''
 Load supporting data (umsl, stop words, negrule, pos-tags

 @author: Riccardo Miotto <rm3086 (at) columbia (dot) edu>
'''

from utility.log import strd_logger
from utility.umls import UmlsDictionary
import utility.file as ufile
import pickle

log = strd_logger('load-data')


# load stop words
def load_stop_words(dstop):
    if dstop is None:
        return None
    eng = ufile.read_file('%s/english.csv' % dstop, 2)
    if not eng:
        eng = set()
    med = ufile.read_file('%s/medical.csv' % dstop, 2)
    if not med:
        med = set()
    pmed = set()
    for m in med:
        pmed.add(m + 's')
    med |= pmed
    stop = (eng, med)
    log.info('loaded %d stopping words' % (len(eng) + len(med)))
    return stop


# load umls dictionary
def load_umls(dumls):
    if dumls is None:
        return None
    umls = UmlsDictionary(dumls)
    log.info('UMLS data: %d dictionary pairs, %d semantic types' % (len(umls.norm), len(umls.stype)))
    return umls


# load part-of-speech tags
def load_pos_tags(fptag):
    if fptag is None:
        return None
    ptag = ufile.read_file(fptag, 2)
    if not ptag:
        return None
    log.info('loaded %d admitted sentence semantic tags' % len(ptag))
    return ptag


# load negation rules
def load_negation_rule(fnegrule):
    if fnegrule is None:
        return None
    negrule = ufile.read_file(fnegrule)
    if not negrule:
        return None
    log.info('loaded %d negation rules' % len(negrule))
    return negrule


# load all supporting files <stop_words, umls, pos-tags, negrule>
def load_data(dstop=None, dumls=None, fptag=None, fnegrule=None):
    stop = load_stop_words(dstop)
    umls = load_umls(dumls)
    ptag = load_pos_tags(fptag)
    negrule = load_negation_rule(fnegrule)

    return (stop, umls, ptag, negrule)
