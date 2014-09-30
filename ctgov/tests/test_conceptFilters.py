"""
    <Module Explanation>
    @author: Praveen Chandar
"""
from unittest import TestCase
from ctgov.concept_mapping.filters import ConceptFilters
import nltk
import string


class TestConceptFilters(TestCase):
    def setUp(self):
        self.stops = (set(['a', 'and', 'the']), set(['heart', 'patient']))
        self.filter = ConceptFilters(ngram_size=5, stopwords=self.stops)

        self.to_remove = string.punctuation.replace('-', '')

    def get_tokens(self, text):
        words = nltk.tokenize.word_tokenize(text)
        wc = [w for w in words if w not in self.to_remove]
        pos = [t[1] for t in nltk.pos_tag(wc)]
        return wc, pos

    def test_accpet_string(self):
        self.filter = ConceptFilters(ngram_size=5, stopwords=self.stops)
        (wc, pos) = self.get_tokens('this is a test string.')
        res = self.filter.accpet_string(wc, pos)
        self.assertTrue(res, ' Testing accept string')

    def test_stopword_test(self):
        self.filter.filter_grammar = False
        self.filter.filter_digit = False
        self.filter.filter_stopwords = True

        (wc, pos) = self.get_tokens('this string 1231 and digit')
        res = self.filter.accpet_string(wc, pos)
        self.assertTrue(res, ' Testing digits: some english stops')

        (wc, pos) = self.get_tokens('the string and heart')
        res = self.filter.accpet_string(wc, pos)
        self.assertFalse(res, ' Testing digits: first cannot be english')

        (wc, pos) = self.get_tokens('string heart and')
        res = self.filter.accpet_string(wc, pos)
        self.assertFalse(res, ' Testing digits: last cannot be english')

        (wc, pos) = self.get_tokens('patient is the string')
        res = self.filter.accpet_string(wc, pos)
        self.assertTrue(res, ' Testing digits: first can be med')

        (wc, pos) = self.get_tokens('1312 problem in heart')
        res = self.filter.accpet_string(wc, pos)
        self.assertTrue(res, ' Testing digits: last can med')

        (wc, pos) = self.get_tokens('the heart')
        res = self.filter.accpet_string(wc, pos)
        self.assertFalse(res, ' Testing digits: all english/med stops')

        (wc, pos) = self.get_tokens('the and')
        res = self.filter.accpet_string(wc, pos)
        self.assertFalse(res, ' Testing digits: all english stops')

        (wc, pos) = self.get_tokens('heart patient')
        res = self.filter.accpet_string(wc, pos)
        self.assertFalse(res, ' Testing digits: all med stops')


    def test_digit_test(self):
        self.filter.filter_grammar = False
        self.filter.filter_stopwords = False
        self.filter.filter_digit = True

        (wc, pos) = self.get_tokens('this string has no digits.')
        res = self.filter.accpet_string(wc, pos)
        self.assertTrue(res, ' Testing digits: full sentence no digits')

        (wc, pos) = self.get_tokens('113 2342 231')
        res = self.filter.accpet_string(wc, pos)
        self.assertFalse(res, ' Testing digits: only digits')

        (wc, pos) = self.get_tokens('113 23.42 23.1')
        res = self.filter.accpet_string(wc, pos)
        self.assertFalse(res, ' Testing digits: only digits with floats')

        (wc, pos) = self.get_tokens('113 the 132.1231 and')
        res = self.filter.accpet_string(wc, pos)
        self.assertFalse(res, ' Testing digits: first or last can not be digits')

        (wc, pos) = self.get_tokens('the 113 and 132.1231')
        res = self.filter.accpet_string(wc, pos)
        self.assertFalse(res, ' Testing digits: first or last can not be digits')

        (wc, pos) = self.get_tokens('some 1231 132.1231 text')
        res = self.filter.accpet_string(wc, pos)
        self.assertTrue(res, ' Testing digits: full sentence with digits')

        self.filter.filter_digit = False
        (wc, pos) = self.get_tokens('113 2342 231')
        res = self.filter.accpet_string(wc, pos)
        self.assertTrue(res, ' Testing digits: checking switch')

    # TODO: Grammar Test case
    def test_grammar_test(self):
        pass