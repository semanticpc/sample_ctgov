# set of general utilities to interact with the Web

# @author: rm3086 (at) columbia (dot) edu

import urllib2, urllib3, json
from log import strd_logger

# logger
log = strd_logger('web')

# get the html source associated to a URL
def download_web_data(url):
    try:
        con = urllib3.connection_from_url(url)
        html = con.urlopen('GET', url, retries=500, timeout=10)
        con.close()
        return html.data
    except Exception as e:
        log.error('%s: %s' % (e, url))
        return None


# get json data
def download_json_data(url):
    try:
        con = urllib2.urlopen(url)
        data = con.read()
        con.close()
        return json.loads(data)
    except Exception as e:
        log.error('%s: %s' % (e, url))
        return None


# clean the html source
def clean_html(html):
    if html is None:
        return None
    return ' '.join(html.replace('\n', '').replace('\t', '').split()).strip()


def clean_text(text):
    """
    Clean the text and takes care of encoding issues

    Shameless copy from django's froce_text function
    """
    encoding = 'utf-8'
    errors = 'strict'
    # Handle the common case first for performance reasons.
    if isinstance(text, unicode):
        return text
    try:
        if not isinstance(text, basestring):
            if hasattr(text, '__unicode__'):
                text = unicode(text)
            else:
                text = unicode(bytes(text), encoding, errors)
        else:
            # Note: We use .decode() here, instead of six.text_type(s, encoding,
            # errors), so that if s is a SafeBytes, it ends up being a
            # SafeText at the end.
            text = text.decode(encoding, errors)
    except UnicodeDecodeError as e:
        if not isinstance(text, Exception):
            raise Exception(text, *e.args)
        else:
            # If we get to here, the caller has passed in an Exception
            # subclass populated with non-ASCII bytestring data without a
            # working unicode method. Try to handle this without raising a
            # further exception by individually forcing the exception args
            # to unicode.
            text = ' '.join([clean_text(arg) for arg in text])
    return text