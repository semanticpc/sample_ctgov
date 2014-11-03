"""
 Extract relevant tags from a text

  @author: Riccardo Miotto <rm3086 (at) columbia (dot) edu>

  Modified by @author: Praveen Chandar
"""

from ctgov.utility.log import strd_logger
from ctgov.utility.web import clean_text
from ctgov.concept_mapping.filters import ConceptFilters
from ctgov.concept_mapping.dict_mapping import DictionaryMapping
from itertools import groupby
import math
import nltk
import string


log = strd_logger('concept-tagger')


class Tagger:
    # constructor
    def __init__(self, ngram=5, stop=None, umls=None, ptag=None):
        self.filter = ConceptFilters(ngram, stop, ptag)
        self.mapper = DictionaryMapping(umls)
        self.ngram = ngram

    def process_text(self, text):
        ptxt = self.process_section(text)
        return ptxt

    def process(self, ec_dict):
        pec = {}
        for it in ec_dict:
            section = ec_dict[it].split(' - ')
            tags = {}
            for s in section:
                # print 'Section:', s
                try:
                    s = str(s)
                except UnicodeEncodeError:
                    s = str(s.encode('utf-8'))
                s = s.lower().strip()
                s = s.replace('- ', ' ').replace(' -', ' ')
                ptxt = self.process_section(s)
                if ptxt is None:
                    continue
                for pp in ptxt:
                    for sent_ptxt in pp:
                        freq = tags.setdefault(sent_ptxt, 0)
                        tags[sent_ptxt] = freq + 1

            if len(tags) == 0:
                continue

            pec[it] = self.substring_filtering(tags, 1)

        # join inclusion/exclusion
        jpec = {}
        for it in pec:
            for t in pec[it]:
                v = jpec.setdefault(t, 0)
                jpec[t] = v + pec[it][t]

        return pec, jpec

    # process the text
    def process_section(self, text):
        section_ptxt = []

        if len(text) == 0:
            return

        to_remove = string.punctuation.replace('-', '')
        sent = clean_text(text).strip()
        sent = nltk.tokenize.sent_tokenize(sent)
        for s in sent:
            # print 'Sentence ', s

            sent_ptxt = []
            words = nltk.tokenize.word_tokenize(s)
            wc = [w for w in words if w not in to_remove]
            pos = [t[1] for t in nltk.pos_tag(wc)]
            if len(wc) != len(pos):
                print w
                print pos

            for i in xrange(len(wc)):
                for j in xrange(i + 1, min(len(wc), i + self.ngram) + 1):
                    toks = wc[i:j]
                    toks_pos = pos[i:j]
                    # Apply the pre-filters
                    if not self.filter.accpet_string(toks, toks_pos):
                        continue

                    # Dictionary Mapping
                    tags = self.mapper.map(toks)
                    if len(tags) != 0:
                        # print 'TAGS-- ', tags, ' ---------- ', toks
                        sent_ptxt += tags

                        # Apply post-filters
                        # if not self.pos_filter.accpet_string(w, pos):
                        #continue
            if len(sent_ptxt) is not 0:
                section_ptxt.append(sent_ptxt)
        return section_ptxt

    def substring_filtering(self, tags, min_c=1):
        substr = {}
        cval = {}
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
            if (t in substr) and (cval[t] < min_c):
                del tags[t]
        return tags