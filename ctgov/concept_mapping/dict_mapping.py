"""
    <Module Explanation>
    @author: Praveen Chandar
"""
import itertools
from ctgov.utility.log import strd_logger

log = strd_logger('dict-mapping')


class DictionaryMapping(object):
    def __init__(self, umls):
        self.use_scramble_find = True
        self.use_split_dashed_words = True
        self.conj = {'and', 'or'}
        self.umls = umls

    def map(self, tokens):
        if not self.umls:
            log.warning('UMLS not loaded')
            return []

        # First do direct mapping
        tags = self._direct_mapping(tokens)

        # If simple direct mapping fails, try other options
        if tags is None and self.use_scramble_find:
            tags = self._scramble_find(tokens)

        # If scrambling fails, look for dashed words
        if tags is None and self.use_split_dashed_words:
            tags = self._split_dashed_words(tokens)

        if tags is None:
            return []
        else:
            return tags

    def _direct_mapping(self, tokens):
        w = ' '.join(tokens).strip()
        if (w not in self.umls.norm) or (len(self.umls.norm[w]) > 5):
            return None

        cl = int(1000)
        wmap = None

        for pt in self.umls.norm[w]:
            dpt = pt.decode('utf-8')
            # Check if the UMLS Concept Norm matches word
            if dpt == w:
                wmap = w
                break

            # An attempt to detect acronyms
            if len(self.umls.norm[w]) > 1:
                acronym_map = self._acronym_greedy(dpt, w)
                if acronym_map is not None:
                    wmap = acronym_map
                    break

            # If nothing works simply return the shortest UMLS Concept that
            # matches the token
            if len(dpt) < cl:
                wmap = dpt
                cl = len(dpt)
        if (wmap not in self.umls.semantic) or (len(self.umls.semantic[wmap] & self.umls.stype) == 0):
            return None
        return [wmap.encode('utf-8').strip()]

    def _scramble_find(self, string):
        if (len(string) > 1) and (len(self.conj & set(string)) > 0):
            stag = set()
            comb = set(itertools.permutations(string))
            for c in comb:
                if len(c) == 1:
                    continue
                t = self._direct_mapping(c)
                if t is not None:
                    if type(t) is list:
                        for i in range(0, len(t)):
                            stag.add(t[i])
                    else:
                        stag.add(t)
            return list(stag)
        else:
            return None

    def _split_dashed_words(self, tokens):
        if len(tokens) > 1:
            return None

        tkn = ' '.join(tokens[0].split('-'))

        local_tags = []
        mt = self._direct_mapping([tkn])
        if mt is not None:
            local_tags += mt
        else:
            if len(tkn) > 1:
                for t in tkn:
                    if len(t) == 0:
                        continue
                    mt = self._direct_mapping([t])
                    if mt is not None:
                        local_tags += mt

        if len(local_tags) != 0:
            return local_tags
        else:
            return None

    # Private Methods
    def _acronym_greedy(self, dpt, tokens):
        tkn = dpt.split()
        if len(tkn) == len(tokens):
            init = set(tokens)
            acr = len(tkn)
            for t in tkn:
                if t[0] in init:
                    acr -= 1
            if acr == 0:
                return dpt
        else:
            return None