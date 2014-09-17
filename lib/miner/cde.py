'''
 	Mine CDEs from a collection of trials associated to a disease

  	@author: Riccardo Miotto
'''

from cvalue import substring_filtering
from lib.utility.log import strd_logger
import math, numpy, operator

log = strd_logger('cde')

'''
	mine the CDEs
'''


def cde_miner(pnct, tags, freq=0.01, umls=None):
    # mine CDEs
    cde = _mine_cde(pnct, freq, tags)
    log.info('------ retained %d CDEs' % len(cde))

    # assign cde to inclusion and exclusion
    ie_cde = {}
    for k, ct in pnct.iteritems():
        for it in ct.pec:
            itdict = ie_cde.setdefault(it, {})
            for t in ct.pec[it]:
                if t in cde:
                    v = itdict.setdefault(t, 0)
                    itdict[t] = v + 1
            ie_cde[it] = itdict

    for it in ie_cde:
        for c in ie_cde[it]:
            v = ie_cde[it][c] / float(len(pnct))
            ie_cde[it][c] = '%.5f' % v

    # cde post-processing
    cde = sorted(cde.iteritems(), key=operator.itemgetter(1), reverse=True)
    cde = _add_semantic_types(cde, umls)

    # inclusion cde post-processing
    cde_inc = None
    if ('inc' in ie_cde) and (len(ie_cde['inc']) > 0):
        cde_inc = sorted(ie_cde['inc'].iteritems(), key=operator.itemgetter(1), reverse=True)
        cde_inc = _add_semantic_types(cde_inc, umls)

    # exclusion cde post-processing
    cde_exc = None
    if ('exc' in ie_cde) and (len(ie_cde['exc']) > 0):
        cde_exc = sorted(ie_cde['exc'].iteritems(), key=operator.itemgetter(1), reverse=True)
        cde_exc = _add_semantic_types(cde_exc, umls)

    return (cde, cde_inc, cde_exc)


# private functions

'''
	add the tag semantic type to the CDEs
'''


def _add_semantic_types(cde, umls):
    ecde = []
    for t in cde:
        stype = []
        if (umls is not None) and (t[0] in umls.semantic):
            stype = sorted(umls.semantic[t[0]] & umls.stype)
        ecde.append([t[0].replace('/', '-'), t[1], ' - '.join(stype)])
    return ecde


'''
	mine the CDEs
'''


def _mine_cde(pdocs, freq, tags):
    cde = {}
    for d in pdocs:

        # gender
        if pdocs[d].gender is not None:
            g = 'gender = %s' % pdocs[d].gender.lower()
            val = cde.setdefault(g, 0)
            cde[g] = val + 1

        # minimum age
        if pdocs[d].minimum_age is not None:
            g = 'minimum age = %s' % pdocs[d].minimum_age
            val = cde.setdefault(g, 0)
            cde[g] = val + 1

        # maximum age
        if pdocs[d].maximum_age is not None:
            g = 'maximum age = %s' % pdocs[d].maximum_age
            val = cde.setdefault(g, 0)
            cde[g] = val + 1

        # ngrams
        for t in pdocs[d].jpec:
            if _check_ngram(t):
                val = cde.setdefault(t, 0)
                cde[t] = val + 1
    log.info('------ obtained %d n-grams' % len(cde))

    # retain the most frequent tags
    if len(pdocs) <= 5:
        fth = 1
    elif len(pdocs) <= 10:
        fth = 2
    elif len(pdocs) < 100:
        fth = 3
    else:
        fth = math.ceil(freq * len(pdocs))

    for t in cde.keys():
        if cde[t] < fth:
            del cde[t]
    log.info('------ retained %d tags appearing at least %d times' % (len(cde), fth))

    # clean the tags
    for t in cde.keys():
        if t not in tags:
            del cde[t]

    cde = substring_filtering(cde, 1000)

    # normalize
    for c in cde:
        v = cde[c] / float(len(pdocs))
        cde[c] = '%.3f' % v

    return cde


'''
	check if age or gender in the free text
'''


def _check_ngram(g):
    w = set(g.split())
    if ('age' in w) or ('gender' in w) or ('genre' in w):
        return False
    if ('man' in w) or ('men' in w) or ('male' in w) or ('males' in w):
        return False
    if ('woman' in w) or ('women' in w) or ('female' in w) or ('females' in w):
        return False
    return True
		


