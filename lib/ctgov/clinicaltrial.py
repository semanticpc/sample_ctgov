'''
    Clinical Trial representation

    @author: Riccardo Miotto
'''

from lib.miner.textprocesser import TextProcesser
from lib.miner.cvalue import substring_filtering
from lib.utility.web import download_web_data
from lib.utility.log import strd_logger
from datetime import datetime
import xml.etree.ElementTree as xml_parser
import math, re

log = strd_logger('clinical-trial')


class ClinicalTrial(object):
    def __init__(self, nctid, data_path):
        self.trail_path = data_path + '/' + nctid + '.xml'
        self.id = nctid
        self.title = None
        self.condition = set()
        self.study_type = None
        self.start_date = None
        self.firstreceived_date = None
        self.verification_date = None
        self.lastchanged_date = None
        self.completion_date = None
        self.gender = None
        self.minimum_age = None
        self.maximum_age = None
        self.ec = None
        self.pec = None
        self.jpec = None


    '''
        download clinical trial and process xml
    '''

    def process(self):

        try:
            xml = xml_parser.fromstring(download_web_data(self.trail_path))

            # general
            self.title = self.__get_info(xml, 'brief_title')
            self.study_type = self.__get_info(xml, 'study_type')
            cond = xml.findall('condition')
            if (cond is not None) and (len(cond) > 0):
                for c in cond:
                    self.condition.add(c.text)
            self.start_date = self.__todate(self.__get_info(xml, 'start_date'))
            self.firstreceived_date = self.__todate(self.__get_info(xml, 'firstreceived_date'))
            self.verification_date = self.__todate(self.__get_info(xml, 'verification_date'))
            self.lastchanged_date = self.__todate(self.__get_info(xml, 'lastchanged_date'))
            self.completion_date = self.__todate(self.__get_info(xml, 'completion_date'))

            # eligibility criteria
            ec = xml.find('eligibility')
            if ec is not None:
                self.gender = self.__get_info(ec, 'gender')
                v = self.__get_info(ec, 'minimum_age')
                self.minimum_age = self.__check_age(v, 'min')
                v = self.__get_info(ec, 'maximum_age')
                self.maximum_age = self.__check_age(v, 'max')
                self.ec = self.__get_ec_text(ec, 'study_pop')
                ec = self.__get_ec_text(ec, 'criteria')
                if ec is not None:
                    if self.ec is None:
                        self.ec = ec
                    else:
                        if 'inc' in ec:
                            s = self.ec.setdefault('inc', '')
                            self.ec['inc'] = s + '. ' + ec['inc']
                        if 'exc' in ec:
                            s = self.ec.setdefault('exc', '')
                            self.ec['exc'] = s + '. ' + ec['exc']
                if self.ec is None:
                    return False
            return True

        except Exception as e:
            log.error('%s --- %s' % (str(e), self.id))
            return False


    '''
        extract n-grams from the eligibility criteria
    '''

    def ec_process(self, stop, umls, ptag):
        if (self.ec is None) or (len(self.ec) == 0):
            return

        self.pec = {}
        for it in self.ec:
            sent = self.ec[it].split(' - ')
            tags = {}
            for s in sent:
                proc = TextProcesser(s, 5, stop, umls, ptag)
                proc.process()
                for pp in proc.ptxt:
                    freq = tags.setdefault(pp, 0)
                    tags[pp] = freq + 1

            if len(tags) == 0:
                continue

            self.pec[it] = substring_filtering(tags, 1)

        # join inclusion/exclusion
        self.jpec = {}
        for it in self.pec:
            for t in self.pec[it]:
                v = self.jpec.setdefault(t, 0)
                self.jpec[t] = v + self.pec[it][t]

        return


    '''
        print out the object
    '''

    def __str__(self):
        out = 'ID: %s \n' % self.id
        out += 'Title: %s \n' % self.title.encode('utf-8')
        out += 'Condition: %s \n' % ' - '.join(self.condition).encode('utf-8')
        out += 'Study Type: %s \n' % self.study_type
        out += 'Start Date: %s \n' % self.start_date
        out += 'First Received Date: %s \n' % self.firstreceived_date
        out += 'Last Changed Date: %s \n' % self.lastchanged_date
        out += 'Verification Date: %s \n' % self.verification_date
        out += 'Completion Date: %s \n' % self.completion_date
        out += 'Gender: %s \n' % self.gender
        out += 'Minimum Age: %s \n' % self.minimum_age
        out += 'Maximum Age: %s \n' % self.maximum_age
        out += 'Eligibility Criteria: %s \n' % self.ec
        out += 'Processed Eligibility Criteria: %s \n' % self.pec
        return out



        # private functions

    '''
        get eligiligty criteria
    '''

    def __get_ec_text(self, ec, field):
        crt = ec.find(field)
        if crt is None:
            return
        ectxt = crt.find('textblock')
        if ectxt is None:
            return
        return self.__preprocess_ec(ectxt.text.encode('utf-8'))


    # pre-process eligiblity criteria to guess inclusion/exclusion
    def __preprocess_ec(self, ec):
        ec = ' '.join(ec.replace('\n', ' ').split())
        stype = {}

        try:
            ec = ec.lower().strip()

            # get inclusion
            re_inc = re.search(r'(inclusion\s*criteria(.*?)(?=[:;\s]){1})', ec, re.S)
            iinc = None
            if re_inc:
                iinc = re_inc.span()

            # get exclusion
            re_exc = re.search(r'(exclusion\s*criteria(.*?)(?=[:;\s]){1})', ec, re.S)
            iexc = None
            if re_exc:
                iexc = re_exc.span()

            # assign text to type
            if (iinc is not None) and (iexc is not None):
                if iinc[0] < iexc[0]:
                    stype['inc'] = ec[iinc[1] + 1:iexc[0] - 1].strip()
                    stype['exc'] = ec[iexc[1] + 1:].strip()
                else:
                    stype['exc'] = ec[iexc[1] + 1:iinc[0] - 1].strip()
                    stype['inc'] = ec[iinc[1] + 1:].strip()

            elif (iinc is None) and (iexc is not None):
                if iexc[0] > 0:
                    stype['inc'] = ec[:iexc[0] - 1].strip()
                stype['exc'] = ec[iexc[1] + 1:].strip()

            else:
                stype['inc'] = ec.strip()

            return stype

        except Exception as e:
            log.error('%s --- %s' % (str(e), self.id))
            return


    '''
        convert string to date
    '''

    def __todate(self, s):
        if s is None:
            return
        s = s.replace(',', '')
        try:
            d = datetime.strptime(s, '%B %d %Y')
            return d.date()
        except ValueError:
            d = datetime.strptime(s, '%B %Y')
            return d.date()
        except Exception as e:
            log.error(e)
            return s


    '''
        get value of a field in the parent tag
    '''

    def __get_info(self, parent, field):
        v = parent.find(field)
        if v is None:
            return
        v = v.text.strip()
        if len(v) == 0:
            return
        return v


    '''
        check the age format
    '''

    def __check_age(self, age, typ):
        try:
            return int(age)
        except ValueError:
            age = age.lower()
            val = age[:age.find(' ')].strip()
            if not val.isdigit():
                return
            val = int(val)
            if 'year' in age:
                return val
            if 'month' in age:
                return self.__format_age(val, typ, float(12))
            if 'week' in age:
                return self.__format_age(val, typ, float(52))
            if 'day' in age:
                return self.__format_age(val, typ, float(365))
        return


    '''
        format the age value
    '''

    def __format_age(self, age, typ, div):
        if typ == 'max':
            return int(math.ceil(age / div))
        if typ == 'min':
            return int(math.floor(age / div))
        return




    