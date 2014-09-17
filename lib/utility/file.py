# set of utilities to interact with files

# @author: rm3086 (at) columbia (dot) edu

import csv, shutil, os, glob, cPickle
from log import strd_logger

# logger
global log
log = strd_logger('file')

# check if a file exist
def file_exist(fname):
    try:
        open(fname, 'r')
        return True
    except IOError:
        return False


# create directory if not existing
def mkdir(dirname):
    if not dirname:
        return False
    try:
        os.makedirs(dirname)
    except OSError:
        pass
    except Exception as e:
        log.error(e)
        return False
    return True


# create directory (delete if one with the same name already exists)
def mk_new_dir(dirname):
    try:
        os.makedirs(dirname)
    except OSError:
        shutil.rmtree(dirname)
        os.makedirs(dirname)
    except Exception as e:
        log.error(e)
        return False
    return True


# copy a file from "source" to "destination"
def fcopy(source, destination):
    try:
        shutil.copy2(source, destination)
    except Exception as e:
        log.error(e)
        return False
    return True


# return the files of a directory with extension "ext"
def flist(directory, ext):
    try:
        os.chdir(directory)
        if ext[0:2] != '*.':
            ext = '*.' + ext
        data = []
        for f in glob.glob(ext):
            data.append(f.strip())
        return data
    except Exception as e:
        log.error(e)
        return None


# ## read operations ###

# read a text file
# @param struct: save data to (1) list, (2) set
def read_file(filename, struct=1, logout=True):
    try:
        fid = open(filename, 'r')
        if struct == 2:
            # set
            data = set()
            for line in fid:
                if len(line) > 0:
                    data.add(line.strip())
        else:
            # default - list
            data = []
            for line in fid:
                if len(line) > 0:
                    data.append(line.strip())
        fid.close()
        return data
    except Exception as e:
        if logout is True:
            log.error(e)
        return None


# read a text file, break lines according to skip
# @param skip: character to skip (default ' ')
def read_file_tokenized(filename, skip=' ', logout=True):
    try:
        data = []
        fid = open(filename, 'r')
        for line in fid:
            line = line.strip()
            data.append(line.split(skip))
        fid.close()
        return data
    except Exception as e:
        if logout is True:
            log.error(e)
        return None


# read text
def read_text(filename, logout=True):
    try:
        fid = open(filename, 'r')
        data = fid.read()
        data = data.replace('\n', ' ').replace('\t', ' ')
        data = ' '.join(data.split()).strip()
        fid.close()
        return data
    except Exception as e:
        if logout is True:
            log.error(e)
        return None


# read data from a csv file
def read_csv(filename, delimiter=',', quotechar='"', compression=None, logout=True):
    try:
        if compression == 'gzip':
            fid = gzip.open(filename, 'r')
        else:
            fid = open(filename, 'r')
        reader = csv.reader(fid, delimiter=delimiter, quotechar=quotechar)
        return list(reader)
    except Exception as e:
        if logout is True:
            log.error(e)
        return


# read a dictionary from a csv file
# @param iKey: column to consider as key (default 0)
# @param iData: column to consider as data (default 1)
def read_csv_as_dict(filename, iKey=0, iData=1, logout=True):
    try:
        reader = csv.reader(open(filename, "r"))
        data = {}
        for r in reader:
            data[r[iKey].strip()] = r[iData].strip()
        return data
    except Exception as e:
        if logout is True:
            log.error(e)
        return None


# read a dictionary from a csv file (column '0' is the keys)
def read_csv_as_dict_with_multiple_items(filename, logout=True):
    try:
        reader = csv.reader(open(filename, "r"))
        data = {}
        for r in reader:
            data[r[0].strip()] = r[1:len(data)]
        return data
    except Exception as e:
        if logout is True:
            log.error(e)
        return None


# read an object (list, dictionary, set) from a serialized file
def read_obj(filename, logout=True):
    try:
        data = cPickle.load(open(filename, 'rb'))
        return data
    except Exception as e:
        if logout is True:
            log.error(e)
        return None


# ## write operations ###

# write data to a csv file
def write_csv(filename, data, logout=True):
    try:
        doc = csv.writer(open(filename, 'wb'), delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        for d in data:
            doc.writerow(d)
        return True
    except Exception as e:
        if logout is True:
            log.error(e)
        return False


# write data to a text file
def write_file(filename, data, logout=True):
    try:
        fid = open(filename, 'w')
        for d in data:
            fid.write('%s\n' % d.encode('utf-8'))
        fid.close()
        return True
    except Exception as e:
        if logout is True:
            log.error(e)
        return False


# write text
def write_text(filename, data, logout=True):
    try:
        fid = open(filename, 'w')
        fid.write('%s' % data.encode('UTF-8', errors='ignore'))
        fid.close()
        return True
    except Exception as e:
        if logout is True:
            log.error(e)
        return False


# write an object (list, set, dictionary) to a serialized file
def write_obj(filename, data, logout=True):
    try:
        cPickle.dump(data, open(filename, 'wb'))
        return True
    except Exception as e:
        if logout is True:
            log.error(e)
        return False








