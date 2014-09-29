"""
    <Module Explanation>
    @author: Praveen Chandar
"""
from ctgov.utility.log import strd_logger
from datetime import datetime
from ctgov.utility.web import clean_text
import xml.etree.ElementTree as xml_parser
import math
import re

log = strd_logger('ctgov-parser')


class ClinicalTrial_Parser(object):
    def __init__(self, data_path):
        self.data_path = data_path

    def parse(self, nct_id):
        try:
            trail_path = self.data_path + '/' + nct_id + '.xml'
            xml = xml_parser.parse(trail_path)

            # general
            doc = {}
            doc['title'] = self.__get_info(xml, 'brief_title')
            doc['study_type'] = self.__get_info(xml, 'study_type')

            # Add conditions
            cond = xml.findall('condition')
            conditions = []
            if (cond is not None) and (len(cond) > 0):
                for c in cond:
                    conditions.append(c.text)
            doc['conditions'] = conditions

            # Dates
            doc['start_date'] = self.__todate(self.__get_info(xml, 'start_date'))
            if not doc['start_date'] == None:
                doc['start_date'] = doc['start_date'].strftime('%Y-%m-%d')

            doc['firstreceived_date'] = self.__todate(self.__get_info(xml, 'firstreceived_date'))
            if not doc['firstreceived_date'] == None:
                doc['firstreceived_date'] = doc['firstreceived_date'].strftime('%Y-%m-%d')

            doc['verification_date'] = self.__todate(self.__get_info(xml, 'verification_date'))
            if not doc['verification_date'] == None:
                doc['verification_date'] = doc['verification_date'].strftime('%Y-%m-%d')

            doc['lastchanged_date'] = self.__todate(self.__get_info(xml, 'lastchanged_date'))
            if not doc['lastchanged_date'] == None:
                doc['lastchanged_date'] = doc['lastchanged_date'].strftime('%Y-%m-%d')

            doc['completion_date'] = self.__todate(self.__get_info(xml, 'completion_date'))
            if not doc['completion_date'] == None:
                doc['completion_date'] = doc['completion_date'].strftime('%Y-%m-%d')


            # eligibility criteria
            ec = xml.find('eligibility')
            if ec is not None:
                doc['ec_gender'] = self.__get_info(ec, 'gender')
                v = self.__get_info(ec, 'minimum_age')
                doc['ec_minimum_age'] = self.__check_age(v, 'min')
                v = self.__get_info(ec, 'maximum_age')
                doc['ec_maximum_age'] = self.__check_age(v, 'max')
                doc['ec_raw_text'] = self.__get_ec_text(ec, 'study_pop')
                ec = self.__get_ec_text(ec, 'criteria')
                if ec is not None:
                    if doc['ec_raw_text'] is None:
                        doc['ec_raw_text'] = ec
                    else:
                        if 'inc' in ec:
                            s = doc['ec_raw_text'].setdefault('inc', '')
                            doc['ec_raw_text']['inc'] = s + '. ' + ec['inc']
                        if 'exc' in ec:
                            s = doc['ec_raw_text'].setdefault('exc', '')
                            doc['ec_raw_text']['exc'] = s + '. ' + ec['exc']
                if doc['ec_raw_text'] is None:
                    log.warning('Eligibility Text not found -- %s' % nct_id)
                    return doc
            return doc

        except Exception as e:
            log.error('%s --- %s' % (str(e), nct_id))
        return False


    def __get_ec_text(self, ec, field):
        crt = ec.find(field)

        if crt is None:
            return
        ectxt = crt.find('textblock')
        if ectxt is None:
            return
        return self.__preprocess_ec(clean_text(ectxt.text))


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

    def __get_info(self, parent, field):
        v = parent.find(field)
        if v is None:
            return
        v = v.text.strip()
        if len(v) == 0:
            return
        return v

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

    def __format_age(self, age, typ, div):
        if typ == 'max':
            return int(math.ceil(age / div))
        if typ == 'min':
            return int(math.floor(age / div))
        return

    def __preprocess_ec(self, ec):
        """
        pre-process eligibility criteria to guess inclusion/exclusion
        :param ec:
        :return:
        """
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
            log.error('%s ' % str(e))
            return