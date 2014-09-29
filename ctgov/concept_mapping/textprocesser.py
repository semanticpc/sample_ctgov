'''
 Extract relevant tags from a text

  @author: Riccardo Miotto <rm3086 (at) columbia (dot) edu>
'''

import nltk, string, itertools
from ctgov.utility.log import strd_logger

log = strd_logger('textprocesser')
conj = set(['and', 'or'])


class TextProcesser:
    # constructor
    def __init__(self, text, ngram=5, stop=None, umls=None, ptag=None):
        try:
            self.text = str(text)
        except UnicodeEncodeError:
            self.text = str(text.encode('utf-8'))
        self.text = self.text.lower().strip()
        self.text = self.text.replace('- ', ' ').replace(' -', ' ')

        # get filtering data
        self.ngr = ngram
        if not stop:
            self.stop = (set(), set())
        else:
            self.stop = stop
        self.umls = umls

        # part of speech tagging
        self.ptag = ptag

        # admitted tags
        self.ptxt = set()


    # process the text
    def process(self):
        if len(self.text) == 0:
            return

        toremove = string.punctuation.replace('-', '')
        sent = self.text.decode('utf-8').strip()
        sent = nltk.tokenize.sent_tokenize(sent)
        for s in sent:
            s = s.decode('utf-8').strip()
            words = nltk.tokenize.word_tokenize(s)
            wc = [w for w in words if w not in toremove]
            pos = [t[1] for t in nltk.pos_tag(wc)]
            if len(wc) != len(pos):
                print w
                print pos
            self.__tag_extraction(wc, pos)

            # Apply the pre-filters

            # Dictionary Mapping

            # Apply post-filters

    # private functions

    # extract tags by ngram analysis
    def __umls_mapping(self, w):
        if self.umls:
            if (w not in self.umls.norm) or (len(self.umls.norm[w]) > 5):
                return None
            us = None
            cl = int(1000)
            wmap = None
            print 'umls norm', self.umls.norm[w]
            for pt in self.umls.norm[w]:
                print pt
                dpt = pt.decode('utf-8')
                # retain same
                if dpt == w:
                    wmap = w
                    break
                # acronym
                if len(self.umls.norm[w]) > 1:
                    tkn = dpt.split()
                    if len(tkn) == len(w):
                        init = set(w)
                        acr = len(tkn)
                        for t in tkn:
                            if t[0] in init:
                                acr -= 1
                        if acr == 0:
                            wmap = dpt
                            break
                # retain shorter
                if (len(dpt) < cl):
                    wmap = dpt
                    cl = len(dpt)
            if (wmap not in self.umls.semantic) or (len(self.umls.semantic[wmap] & self.umls.stype) == 0):
                return None
            return wmap.encode('utf-8').strip()
        else:
            return w


    # standardize text by umls dictionary and semantic type
    def __tag_extraction(self, words, pos):
        for i in xrange(len(words)):
            for j in xrange(i + 1, min(len(words), i + self.ngr) + 1):

                # grammar validity
                if len(words[i:j]) == 1:
                    if (self.ptag is not None) and (not self.__check_grammar(i, pos)):
                        continue

                # mapping
                t = self.map_ngram(words[i:j])
                if t is not None:
                    self.ptxt.add(t)
                elif (len(words) > 1) and (len(conj & set(words[i:j])) > 0):
                    # analyze inner patterns with conjunctions
                    stag = self.__scramble_ngram(words[i:j])
                    if len(stag) > 0:
                        self.ptxt |= stag

                # analyze single words with '-' inside
                if (len(words[i:j]) > 1):
                    continue
                tkn = ' '.join(words[i:j]).split('-')
                if len(tkn) > 1:
                    for t in tkn:
                        if len(t) == 0:
                            continue
                        mt = self.map_ngram([t])
                        if mt is not None:
                            self.ptxt.add(mt)
        return

    # check tag validity
    def __check_tags(self, tag):
        if tag is None:
            return False
        words = tag.split()
        if len(words) > self.ngr:
            return False
        if len(words) == 1:
            if self.__check_word(tag) is False:
                return False
        else:
            if (words[0].isdigit()) or (words[-1].isdigit()):
                return False
            if (words[0] in self.stop[0]) or (words[-1] in self.stop[0]):
                return False
            cstop = 0
            if len(words) >= 3:
                for w in words:
                    if self.__check_word(w) is False:
                        cstop += 1
                if cstop == len(words):
                    return False
        return True


    # check word validity
    def __check_word(self, w):
        if w.isdigit():
            return False
        if len(w) == 1:
            return False
        if (w in self.stop[0]) or (w in self.stop[1]):
            return False
        return True


    # check word grammar validity
    def __check_grammar(self, iword, tpos):
        if tpos[iword] in self.ptag:
            return False
        return True


    # analyze scrambled ngrams
    def __scramble_ngram(self, words):
        stag = set()
        comb = set(itertools.permutations(words))
        for c in comb:
            if len(c) == 1:
                continue
            t = self.map_ngram(c)
            if t is not None:
                stag.add(t)
        return stag


    # map the ngram to tag
    def map_ngram(self, words):
        w = ' '.join(words).strip()
        if not self.__check_tags(w):
            return None
        t = self.__umls_mapping(w)
        if not self.__check_tags(t):
            return None
        return t
