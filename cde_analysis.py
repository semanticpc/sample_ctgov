"""
    Time Analysis of CDEs
    @author: Riccardo Miotto
"""

import argparse
from matplotlib.backends.backend_pdf import PdfPages
from pylab import *
import numpy as np
import matplotlib.pyplot as plt
from ctgov.utility.log import strd_logger
import ctgov.utility.file as ufile


log = strd_logger('cde-time-analysis')


def cde_analysis(ddata, dout, ystep=1):
    if ystep < 1:
        log.error('the year step needs to be greater than 1 -- interrupting')
        return

    dout = '%s/year-step-%d' % (dout, ystep)
    if not ufile.mkdir(dout):
        log.error('impossible to create the output directory - interrupting')
        return

    # get list of diseases
    ddata = '%s/year-step-%d' % (ddata, ystep)
    ldis = sorted(os.walk(ddata).next()[1])

    yinterval = _year_interval(ystep)
    cde = {}
    year = {}
    disease = {}
    jcde = {}
    stype = {}
    for d in ldis:
        ddis = '%s/%s' % (ddata, d)
        for y in yinterval:
            ycde = ufile.read_csv('%s/%s.csv' % (ddis, y), logout=False)
            if ycde is None:
                continue

            ycde = ycde[1:]
            for yc in ycde:

                # process semantic type
                val = stype.setdefault(yc[0], set())
                typ = yc[2].split('-')
                for t in typ:
                    val.add(t.strip())
                stype[yc[0]] |= val

                # by-cde
                cde = _update_data(cde, yc[0], d, (y, yc[1]))

                # by-year
                year = _update_data(year, y, yc[0], (d, yc[1]))

                # by-disease
                disease = _update_data(disease, d, yc[0], (y, yc[1]))

    # save CDEs sorted by disease-coverage
    log.info('saving CDEs sorted by disease coverage \n')
    _save_cde(cde, stype, dout)

    # save matrix
    log.info('processing data by cde: saving matrices disease - year \n')
    dbycde = '%s/by-cde' % dout
    ufile.mkdir(dbycde)
    _define_occurrence(cde, disease.keys(), year.keys(), dbycde, True)

    # log.info ('processing data by year: saving matrices cde - disease \n')
    # dbyyear = '%s/by-year' % dout
    # ufile.mkdir (dbyyear)
    # _define_occurrence (year, cde.keys(), disease.keys(), dbyyear, False)

    # log.info ('processing data by disease: saving matrices cde - year \n')
    # dbydisease = '%s/by-disease' % dout
    # ufile.mkdir (dbydisease)
    # _define_occurrence (disease, cde.keys(), year.keys(), dbydisease, False)

    # joined cde/disease occurrence
    # log.info ('saving joined data: cde - disease')
    # _define_joined_occurrence (cde, cde.keys(), disease.keys(), dout)

    return


# private functions

'''
	save CDEs sorted by disease coverage
'''


def _save_cde(cde, stype, dout):
    scde = []
    for c in cde:
        if c in stype:
            st = ' - '.join(stype[c])
        else:
            st = ''
        scde.append((c, len(cde[c]), st))
    scde.sort(key=lambda x: x[1], reverse=True)
    scde.insert(0, ('CDE', 'No. of Disease', 'Semantic Type'))
    filename = '%s/cde-list.csv' % dout
    ufile.write_csv(filename, scde)
    return


'''
	define cde/disease joined occurrence
'''


def _define_joined_occurrence(jcde, cde, disease, dout):
    matr = []
    for c in sorted(cde):
        row = []
        for d in sorted(disease):
            iyear = dict([(y, '0.00') for y in xrange(1999, 2014)])
            if d in jcde[c]:
                for y in jcde[c][d]:
                    iyear[y[0]] = y[1]
            row.append('/'.join([v for k, v in sorted(iyear.iteritems())]))
        row.insert(0, c)
        matr.append(row)
    matr.insert(0, [''] + sorted(disease))
    filename = '%s/joined-cde-disease.csv' % dout
    ufile.write_csv(filename, matr)
    return


'''
	define occurrence matrix
'''


def _define_occurrence(data, hrow, hcol, dout, plot):
    dcsv = '%s/csv' % dout
    dpdf = '%s/pdf' % dout
    ufile.mkdir(dcsv)
    ufile.mkdir(dpdf)

    for k1 in sorted(data.keys()):
        matr = [[''] + list(sorted(hcol))]
        for r in sorted(hrow):
            col = dict([(h, 0) for h in hcol])
            if r in data[k1]:
                for y in data[k1][r]:
                    # if y[0] not in col:
                    # print y[0]
                    col[y[0]] = float(y[1])
            lcol = [col[c] for c in sorted(col)]
            lcol.insert(0, r)
            matr.append(lcol)

        # save csv
        name = str(k1).replace('/', '-')
        ufile.write_csv('%s/%s.csv' % (dcsv, name), matr)

        # save pdf
        if plot:
            _plot_matrix(matr, '%s/%s.pdf' % (dpdf, name))

    return


'''
	plot matrix
'''


def _plot_matrix(matr, filename):
    matr = matr[1:]
    matr = [m[1:] for m in matr]
    pdf = PdfPages(filename)
    xtick = [i for i in xrange(1999, 2014)]
    fig, ax = plt.subplots()
    pic = ax.pcolor(np.array(matr), cmap=plt.cm.Blues)
    ax.set_xlim([0, len(xtick)])
    ax.set_ylim([0, len(matr)])
    ax.set_xticks(np.arange(len(xtick)))
    ax.set_xticklabels(xtick, rotation=45, fontsize=8, ha='left')
    ax.set_yticks([])
    ax.set_yticklabels([])
    grid(True)
    ylabel('Cancer Type', fontsize=10)
    savefig(pdf, format='pdf', bbox_inches='tight')
    pdf.close()
    close()
    return


'''
	update dictionary
'''


def _update_data(d, k1, k2, val):
    stat = d.setdefault(k1, {})
    stat.setdefault(k2, set()).add(val)
    d[k1] = stat
    return d


'''
	define the interval of years for mining
'''


def _year_interval(ystep):
    first = 1999
    last = 2013
    yi = []
    while first <= last:
        if ystep > 1:
            ilast = first + ystep - 1
            yi.append('%s-%s' % (first, ilast))
            first = ilast + 1
        else:
            yi.append(first)
            first += 1
    return yi


'''
	main function
'''

# processing the command line options
def _process_args():
    parser = argparse.ArgumentParser(description='Time Analysis of CDEs')
    parser.add_argument(dest='d', help='data directory')
    parser.add_argument(dest='o', help='output directory')
    parser.add_argument('-y', default=1, type=int, help='year step (default: 1)')
    return parser.parse_args(sys.argv[1:])


if __name__ == '__main__':
    args = _process_args()
    print ''
    cde_analysis(args.d, args.o, args.y)
    log.info('task completed')
    print ''
