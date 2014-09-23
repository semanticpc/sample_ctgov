'''
 Filter substrings according to c-value coefficient

 @author: Riccardo Miotto <rm3086 (at) columbia (dot) edu>
 '''

import math
from itertools import groupby

# @param tags: {tag, freq}
# @param minc: c-value threshold value
def substring_filtering(tags, minc=1):
    substr = {}
    cval = {}
    words = tags
    sfeat = [(k, v) for k, v in reversed(sorted(tags.iteritems(), key=lambda x: len(x[0].split())))]
    for k, gr in groupby(sfeat, lambda x: len(x[0].split())):
        for sf in reversed(sorted(gr, key=lambda x: x[1])):
            if sf[0] not in substr:
                cval[sf[0]] = sf[1] * math.log(len(sf[0].split()), 2)
            else:
                fr = sf[1] - (substr[sf[0]][1] / float(substr[sf[0]][2]))
                ln = len(sf[0].split())
                if ln == 1:
                    ln += 0.1
                cval[sf[0]] = math.log(ln, 2) * fr
            tk = sf[0].split()
            for i in xrange(len(tk)):
                for j in xrange(i + 1, len(tk) + 1):
                    sub = ' '.join(tk[i:j])
                    if (sub != sf[0]) and (sub in tags):
                        val = substr.setdefault(sub, (tags[sub], 0, 0))
                        upd = sf[1]
                        if sf[0] in substr:
                            upd -= substr[sf[0]][1]
                        substr[sub] = (val[0], val[1] + upd, val[2] + 1)
    # filter substrings
    for t in tags.keys():
        if (t in substr) and (cval[t] < minc):
            del tags[t]
    return tags
