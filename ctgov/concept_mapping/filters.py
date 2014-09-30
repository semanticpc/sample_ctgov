"""
    <Module Explanation>
    @author: Praveen Chandar
"""

# Static Methods to create pre and post filters


def get_pre_concept_filter(ngram_size=5, stopwords=None, ptags=None):
    pass


def get_post_concept_filter(ngram_size=5, stopwords=None, ptags=None):
    pass


class ConceptFilters(object):
    def __init__(self, ngram_size=5, stopwords=None, ptags=None):
        # get filtering data
        self.ngram_size = ngram_size

        # Stopwords
        if not stopwords:
            self.stopwords = (set(), set())
        else:
            self.stopwords = stopwords

        # POS tags for tag filtering
        if ptags is None:
            self.filter_grammar = False
        else:
            self.ptags = ptags
            self.filter_grammar = True

        # Filter switches
        self.filter_digit = True
        self.filter_stopwords = True


    def accpet_string(self, tokens, pos):
        # Returns True to accept string
        # False to reject the string

        if tokens is None:
            return False

        # Checks if the string length if within the ngram limit
        if len(tokens) > self.ngram_size:
            return False

        # If the string to test is a single word
        # Apply the following filters first
        if len(tokens) == 1 and self.filter_grammar:
            if not self.grammar_test(pos[0]):
                return False

        if self.digit_test(tokens) == 'Failed Test':
            return False

        if self.stopword_test(tokens) == 'Failed Test':
            return False

        return True

    def digit_test(self, tokens):
        # Check if the test is enabled
        if not self.filter_digit:
            return 'Passed Test'

        first = tokens[0]
        last = tokens[-1]

        # First or last cannot be a digit
        if self._check_digit(first) or self._check_digit(last):
            return 'Failed Test'


        # Check if all tokens digits
        cstop = 0
        for w in tokens:
            if w.isdigit():
                cstop += 1
        if cstop == len(tokens):
            return 'Failed Test'

        return 'Passed Test'

    def _check_digit(self, x):
        try:
            a = float(x)
            b = int(a)
        except (ValueError, OverflowError) as e:
            return False
        else:
            return True

    def stopword_test(self, tokens):
        # Check if the test is enabled
        if not self.filter_stopwords:
            return 'Passed Test'

        first = tokens[0]
        last = tokens[-1]

        # First or last cannot be an ENGLISH stopwords
        if (first in self.stopwords[0]) or (last in self.stopwords[0]):
            return 'Failed Test'

        # Check if all tokens are ENGLISH or MEDICAL stopwords
        cstop = 0
        for w in tokens:
            if (w in self.stopwords[0]) or (w in self.stopwords[1]):
                cstop += 1
        if cstop == len(tokens):
            return 'Failed Test'
        return 'Passed Test'

    def grammar_test(self, pos):
        """
        Only for single word strings
        Checks if the the POS of the string is allowed

        :param pos:
        :return:
        """
        # Check if the test is enabled
        if not self.filter_grammar:
            return True

        if pos in self.ptags:
            return False
        return True