"""
    <Module Explanation>
    @author: Praveen Chandar
"""
from unittest import TestCase
from ctgov.utility.umls import UmlsDictionary
from ctgov.concept_mapping.dict_mapping import DictionaryMapping
import nltk
import string


class TestDictionaryMapping(TestCase):
    def setUp(self):
        # self.umls = UmlsDictionary('/Users/praveen/work/code/ctgov/resources/umls_sample')
        self.umls = UmlsDictionary('/Users/praveen/work/code/ctgov/resources/umls_sample')
        self.mapper = DictionaryMapping(self.umls)

    def get_tokens(self, text):
        to_remove = string.punctuation.replace('-', '')
        words = nltk.tokenize.word_tokenize(text)
        wc = [w for w in words if w not in to_remove]
        return wc

    def test_map(self):
        self.mapper.use_scramble_find = False
        self.mapper.use_split_dashed_words = True

        wc = self.get_tokens('history of liver disease, or elevated liver function tests')
        print wc
        print self.mapper.map(wc[1:5])



        # def test__scramble_find(self):
        # self.fail()
        #
        # def test__split_dashed_words(self):
        # self.fail()