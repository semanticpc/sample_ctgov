'''
	UMLS dictionary data structure
	
	@author: Riccardo Miotto
'''

import lib.utility.file as ufile


class UmlsDictionary:
    '''
        constructor

        @var norm: map "sentence" to "preferred sentence"
        @var semantic: map "preferred sentence" to "semantic types"
        @var stype: list of semantic types
        @var stype2scategory: map "semantic type" to "semantic category"
        @var scategory: list of semantic categories
    '''

    def __init__(self, dumls=None):
        self.norm = {}
        self.semantic = {}
        self.stype = set()
        self.typ2cat = {}
        self.scategory = set()

        # load from file
        if dumls is not None:
            if not dumls.endswith('/'):
                dumls += '/'
            self.__load_from_file(dumls)


    # load umsl data from files stored in 'dumls'
    def __load_from_file(self, dumls):
        # load categories
        st = ufile.read_file(dumls + 'umls-semantic.csv', 2)
        if st is not None:
            self.stype = set([c.lower() for c in st])
        else:
            self.stype = set()
        # load dictionary
        udct = ufile.read_csv(dumls + 'umls-dictionary.csv')
        if udct is not None:
            for u in udct:
                # semantic types
                stype = set(u[2].strip().split('|'))
                # preferred terms
                pterms = u[1].strip().split('|')
                ns = set()
                for pt in pterms:
                    ns.add(pt)
                    sty = self.semantic.setdefault(pt, set())
                    sty |= stype
                    self.semantic[pt] = sty
                if len(ns) > 0:
                    self.norm[u[0].strip()] = ns

        cat = ufile.read_csv('%s/umls-semantic-groups.txt' % dumls, delimiter='|', quotechar=None)
        for c in cat:
            lc = c[3].lower()
            if lc in self.stype:
                lg = c[1].lower()
                self.typ2cat[lc] = lg
                self.scategory.add(lg)


    # set variables
    def set_normalizer(self, nm):
        self.norm = nm


    def set_semantic_map(self, smap):
        self.semantic_map = smap


    def set_semantic_type(self, stype):
        self.semantic_type = stype


    # retrieve the semantic type of a term
    def retrieve_semantic_category(self, c):
        if c in self.semantic:
            return sorted(self.semantic[c])
        return None
		



