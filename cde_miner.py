'''
 	Mine CDEs for a set of disease
 	@param yearstep: step for time analysis

 	@author: Riccardo Miotto
'''

from lib.utility.log import strd_logger
from lib.miner.cde import cde_miner
from lib.load_data import load_umls
from datetime import timedelta
import lib.utility.file as ufile
import argparse, sys, math, datetime, zipfile, os, shutil

log = strd_logger('cde-miner')


def mining_cde(nct, disease, nctmin=100, fth=0.03, umls=None, dout=None, yearstep=-1):
    if yearstep <= 0:
        yearstep = -1

    # get year interval
    yi = _year_interval(yearstep)

    # check output directory
    if not _check_dout(dout):
        return
    if yearstep == -1:
        dout = '%s/all-years' % dout
    else:
        dout = '%s/year-step-%d' % (dout, yearstep)
    if not ufile.mkdir(dout):
        log.error('impossible to create the output directory -- interrupting')
        return

    # process each disease
    log.info('found %d diseases' % len(disease))
    kdis = sorted([k for k in disease if len(disease[k]) >= nctmin])
    log.info('--- retained %d diseases with more than %d trial associated \n' % (len(kdis), nctmin))

    for k in sorted(kdis):

        log.info('processing: "%s" (%d trials)' % (k, len(disease[k])))
        cde = {}

        ddis = '%s/%s' % (dout, k.replace(' ', '-').replace('/', '-'))

        # process each year interval
        for y in yi:
            log.info('--- year interval: %s - %s' % (y[0], y[1]))
            dnct = {}
            tags = {}
            for d in disease[k]:

                # check validity
                if d not in nct:
                    continue

                if len(nct[d].condition) > 1:
                    continue

                if (nct[d] is None) or (nct[d].pec is None):
                    continue
                rdate = nct[d].firstreceived_date
                if rdate is None:
                    continue
                if (rdate.year < y[0]) or (rdate.year > y[1]):
                    continue

                # get ngrams
                for t in nct[d].jpec:
                    v = tags.setdefault(t, 0)
                    tags[t] = v + 1
                dnct[d] = nct[d]

            if len(dnct) == 0:
                log.info('------ no trials found')
                cde[y] = (0, [])
                continue
            log.info('------ found %d clinical trials' % len(dnct))

            # mine the CDEs
            cde[y] = (len(dnct), cde_miner(dnct, tags, fth, umls))

        # save
        ufile.mkdir(ddis)
        saved = _save_cde(cde, k, yearstep, ddis)

        # get association tag - clinical trial
        if (yearstep == -1) and (saved):
            cde_to_trial = {}
            cde_list = [c[0] for c in cde.itervalues().next()[1][0]]
            for c in cde_list:
                for ct in dnct:
                    if c in dnct[ct].jpec:
                        cde_to_trial.setdefault(c, set()).add(ct)

            # save
            dout_trial = '%s/cde-to-trial' % ddis
            ufile.mkdir(dout_trial)
            for c in cde_to_trial:
                ufile.write_file('%s/%s.txt' % (dout_trial, c), sorted(cde_to_trial[c]))

            # zip
            fzip = '%s/cde-to-trial.zip' % ddis
            zp = zipfile.ZipFile(fzip, 'w')
            for root, dirs, files in os.walk(dout_trial):
                for f in files:
                    zp.write(os.path.join(root, f), os.path.basename(f))
            zp.close()
            shutil.rmtree(dout_trial)

        print ''

    return cde


# private functions

'''
	define the interval of years for mining
'''


def _year_interval(ystep):
    first = 1998
    last = 2013
    if ystep == -1:
        return [(first, last)]
    yi = []
    while first <= last:
        ilast = first + ystep - 1
        yi.append((first, ilast))
        first = ilast + 1
    return yi


# check output directory
def _check_dout(d):
    if d is None:
        log.error('no output directory specified - interrupting')
        return False
    if not ufile.mkdir(d):
        log.error('impossible to create the output directory - interrupting')
        return False
    return True


'''
	save CDEs for a disease
'''


def _save_cde(cde, name, ystep, dout):
    if ystep == -1:
        cde = cde.itervalues().next()[1]
        if not _write_cde(cde, dout):
            shutil.rmtree(dout)
            return False

    else:
        nnct = [('Year Interval', 'No. of Trials')]
        saved = 0
        for c in sorted(cde):
            if ystep == 1:
                nnct.append((c[0], cde[c][0]))
            else:
                nnct.append(('%s - %s' % (c[0], c[1]), cde[c][0]))

            if cde[c][0] == 0:
                continue

            data = list(cde[c][1][0])
            data.insert(0, ['CDEs', 'Frequency (0-1 normalized)', 'UMLS Semantic Types'])
            if ystep == 1:
                fout = '%s/%s.csv' % (dout, c[0])
            else:
                fout = '%s/%s-%s.csv' % (dout, c[0], c[1])
            if ufile.write_csv(fout, data):
                saved += 1
        if saved > 0:
            ufile.write_csv('%s/trial-per-year.csv' % dout, nnct)
        else:
            shutil.rmtree(dout)
            return False
    return True


'''
	write CDEs for a disease with division between inclusion and exclusion (only for all years)
'''


def _write_cde(cde, dout):
    saved = False
    if (cde[0] is not None) and (len(cde[0]) > 0):
        cde[0].insert(0, ['CDEs', 'Frequency (0-1 normalized)', 'UMLS Semantic Types'])
        ufile.write_csv('%s/cde-list.csv' % dout, cde[0])
        saved = True
    if (cde[1] is not None) and (len(cde[1]) > 0):
        cde[1].insert(0, ['CDEs', 'Frequency (0-1 normalized)', 'UMLS Semantic Types'])
        ufile.write_csv('%s/cde-inclusion.csv' % dout, cde[1])
    if (cde[2] is not None) and (len(cde[2]) > 0):
        cde[2].insert(0, ['CDEs', 'Frequency (0-1 normalized)', 'UMLS Semantic Types'])
        ufile.write_csv('%s/cde-exclusion.csv' % dout, cde[2])
    return saved


'''
	main function
'''

# processing the command line options
def _process_args():
    parser = argparse.ArgumentParser(description='Mine CDEs for a set of diseases')

    # clinical trial file
    parser.add_argument(dest='nct', help='clinical trial file')

    # disease - nct association file
    parser.add_argument(dest='dis', help='disease-nct association file')

    # minimum number of clinical trials for a disease
    parser.add_argument('-m', default=100, type=int, help='min number of trials for a disease (default: 100)')

    # output directory
    parser.add_argument('-o', default=None, help='output directory (default: None)')

    # frequency threshold
    parser.add_argument('-t', default=0.03, type=float, help='frequency threshold (default: 0.03)')

    # umls directory
    parser.add_argument('-u', default=None, help='umls directory (default: None)')

    # year step
    parser.add_argument('-y', default=-1, type=int, help='year step for time analysis (default: -1 (all))')

    return parser.parse_args(sys.argv[1:])


if __name__ == '__main__':
    args = _process_args()
    print ''

    umls = load_umls(args.u)

    nct = ufile.read_obj(args.nct)

    if nct is None:
        log.error('impossible to load clinical trials - interrupting')
        sys.exit()

    disease = ufile.read_obj(args.dis)
    if disease is None:
        log.error('impossible to load the disease - trial association  - interrupting')
        sys.exit()

    mining_cde(nct, disease, args.m, args.t, umls, args.o, args.y)
    log.info('task completed')
    print ''


		