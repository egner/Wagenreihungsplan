#!/usr/bin/env python2.7
# -*- mode: python; coding: us-ascii; indent-tabs-mode: nil -*-
# vim: set filetype=python fileencoding=utf-8 expandtab sw=4 sts=4:
#
# Bahn Wagenreihung (aka. Wagenstandsanzeiger).
# SE, created 20-Jun-2016, in Python 2.7.11.
#
# References:
# [1] http://data.deutschebahn.com/dataset/data-wagenreihungsplan-soll-daten

# This application prints the location of a certain carriage of a certain
# train at all stations of call.

# try:
# ./wagenreihungsplan.py -t
# ./wagenreihungsplan.py -s 415 23

import argparse
import bz2
import codecs
import datetime
import logging
import os
import os.path
import cPickle
import re
import sys
import xml.dom.minidom

# -- General utilities --

sys.dont_write_bytecode = True # don't bother with .pyc-files

_FIRST_TIMESTAMP = None

def timestamp():
    global _FIRST_TIMESTAMP
    t = datetime.datetime.utcnow()
    if _FIRST_TIMESTAMP is None:
        _FIRST_TIMESTAMP = t
        return 'T=%s' % t.strftime('%Y%m%dT%H%M%SZ')
    else:
        dt = (t - _FIRST_TIMESTAMP).total_seconds()
        return 'T + %.1fs' % dt

# -- Utilities for XML --

def text(x):
    "Get all text inside a node."
    if isinstance(x, list):
        return ''.join([text(xi) for xi in x])
    elif isinstance(x, xml.dom.minidom.Text):
        assert isinstance(x.data, basestring)
        return x.data
    elif isinstance(x, xml.dom.minidom.Element):
        return text(x.childNodes)
    else:
        raise Exception('Unrecognized type of object %r' % (x,))

def text_by_tag(x, tagName, sep=None):
    "Get text from child nodes, joined with separator."
    children = [text(c) for c in x.childNodes
                if isinstance(c, xml.dom.minidom.Element)
                and c.tagName == tagName]
    assert len(children) >= 1, (x, tagName)
    assert len(children) == 1 or sep is not None, (x, tagName, sep)
    if sep is None:
        return children[0]
    else:
        return sep.join(children)


# -- Decoding XML from 'Wagenreihungsplan_RawData_*/*.xml' --

def read_trainNumbers(datasetdir): # -> yields trainNumber elements
    infiles = sorted(os.listdir(datasetdir))
    # infiles = ['BLS_2017-11-24_12-28-07.xml', 'MH_2017-12-01_09-31-22.xml']
    for infile in infiles:
        if infile.endswith('.xml'):
            inpath = os.path.abspath(os.path.join(datasetdir, infile))
            logging.info('Reading %r.', os.path.relpath(inpath, datasetdir))
            dom = xml.dom.minidom.parse(os.path.join(datasetdir, infile))
            for trainNumber in dom.getElementsByTagName('trainNumber'):
                yield trainNumber

_INT_RE = re.compile('^[0-9]+$')

def decode_trainNumber(s):
    s = s.strip().lower()
    if _INT_RE.match(s):
        return int(s)
    else:
        return s

def read_dataset(datasetdir):
    for trainNumber in read_trainNumbers(datasetdir):
        train_trainNumber = decode_trainNumber(text(trainNumber))

        train = trainNumber.parentNode.parentNode
        assert train.tagName == 'train'
        train_name = text_by_tag(train, 'name')
        train_time = text_by_tag(train, 'time')

        track = train.parentNode.parentNode
        assert track.tagName == 'track'
        track_name = text_by_tag(track, 'name')

        station = track.parentNode.parentNode
        assert (station.tagName == 'station')
        station_shortcode = text_by_tag(station, 'shortcode')
        station_name = text_by_tag(station, 'name')

        for waggon in train.getElementsByTagName('waggon'):
            waggon_number = text_by_tag(waggon, 'number').lower()

            for section in waggon.getElementsByTagName('sections'):
                section_identifiers = text_by_tag(section, 'identifier', sep='/')
                yield {'train.trainNumber': train_trainNumber,
                       'train.name': train_name,
                       'train.time': train_time,
                       'track.name': track_name,
                       'station.shortcode': station_shortcode,
                       'station.name': station_name,
                       'waggon.number': waggon_number,
                       'section.identifiers': section_identifiers}


# -- Compile dataset into pickle --

def compile_dataset(datasetdir):
    "Read dataset from XML, extracting entries."
    if datasetdir is None:
        raise ValueError('Need a path to a dataset.')

    datasetpath = os.path.abspath(datasetdir)
    logging.info('Processing dataset %r, at %s.', datasetpath, timestamp())

    ds = {}
    ds['$type'] = _TYPE
    ds['$dataset'] = datasetpath
    ds['entries'] = sorted(list(read_dataset(datasetpath)))
    return ds

_TYPE = 'Wagenreihungsplan' # The tag to identify our datasets.

def load_dataset(datasetdir, picklefile):
    "Load dataset from picklefile, or recreate it from datasetdir."
    picklepath = os.path.abspath(picklefile)
    datasetpath = None
    if datasetdir is not None:
        datasetpath = os.path.abspath(datasetdir)

    ds = None
    if os.path.isfile(picklepath):
        logging.debug('Loading compiled dataset %r at %s.', picklepath, timestamp())
        with bz2.BZ2File(picklepath, 'rb') as inp:
            ds = cPickle.load(inp)

    is_valid = ( isinstance(ds, dict) and
                 ds.has_key('$type') and ds['$type'] == _TYPE and
                 ds.has_key('$dataset') and
                 (datasetpath is None or ds['$dataset'] == datasetpath) )
    if not is_valid:

        ds = compile_dataset(datasetpath)

        pickledir = os.path.dirname(picklepath)
        if not os.path.isdir(pickledir):
            logging.info('Creating directory %r.', pickledir)
            os.makedirs(pickledir)

        logging.info('Saving compiled dataset %r, at %s.', picklepath, timestamp())
        with bz2.BZ2File(picklepath, 'wb') as out:
            cPickle.dump(ds, out)

    logging.debug('Dataset loaded at %s.', timestamp())
    return ds


# -- Table output --

def field_widths(table):
    fws = []
    for row in table:
        for i in range(len(row)):
            if not i < len(fws):
                fws.append(0)
            fws[i] = max(fws[i], len(row[i]))
    return fws

def print_table(table):
    fws = field_widths(table)
    for row in table:
        rowstr = []
        for i in range(len(fws)):
            f = ''
            if i < len(row):
                f = row[i]
            padding = fws[i] - len(f) + 2
            out = codecs.encode(f + ' '*padding, 'UTF-8')
            sys.stdout.write(out)
        sys.stdout.write('\n')


# -- Use compiled dataset --

_INVALID_TRAIN_TIME = '99:99:00'

def list_trains(datasetdir, picklefile):
    ds = load_dataset(datasetdir, picklefile)

    tns = set()
    first_by_tn = {}
    last_by_tn = {}
    for e in ds['entries']:
        tn = e['train.trainNumber']
        tt = e['train.time']
        if tt != _INVALID_TRAIN_TIME:
            if tn in tns:
                if tt < first_by_tn[tn]['train.time']:
                    first_by_tn[tn] = e
                if tt > last_by_tn[tn]['train.time']:
                    last_by_tn[tn] = e
            else:
                first_by_tn[tn] = e
                last_by_tn[tn] = e
            tns.add(tn)

    table = [['Zug', 'Zugname', 'Ab', 'Von', 'An', 'Nach']]
    for tn in sorted(list(tns)):
        first = first_by_tn[tn]
        last = last_by_tn[tn]

        row = []
        row.append(str(tn))
        row.append(first['train.name'])
        row.append(first['train.time'])
        row.append('%s (%s)' % (first['station.name'], first['station.shortcode']))
        row.append(last['train.time'])
        row.append('%s (%s)' % (last['station.name'], last['station.shortcode']))
        table.append(row)

    print_table(table)

def list_section(trainNumber, waggon, datasetdir, picklefile):
    ds = load_dataset(datasetdir, picklefile)
    es1 = [ e for e in ds['entries']
            if e['train.trainNumber'] == trainNumber and
            e['train.time'] != _INVALID_TRAIN_TIME ]
    if len(es1) == 0:
        print 'Zug %r nicht gefunden.' % (trainNumber,)
        return False
    es2 = [e for e in es1 if e['waggon.number'] == waggon]
    if len(es2) == 0:
        print 'Wagen %r von Zug %r nicht gefunden.' % (waggon, trainNumber)
        return False

    es3 = sorted([(e['train.time'], e) for e in es2])
    es4 = [e for (_unused_time, e) in es3]

    table = [['Ab', 'Von', 'Gleis', 'Abschnitt']]
    for e in es4:
        row = []
        row.append(e['train.time'])
        row.append('%s (%s)' % (e['station.name'], e['station.shortcode']))
        row.append(e['track.name'])
        row.append(e['section.identifiers'])
        table.append(row)

    print_table(table)
    return True

def main():
    pickle = 'Wagenreihungsplan.pickle.bz2'

    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--trains', action='store_true',
                        help='list trains and exit')
    parser.add_argument('-s', '--section', nargs=2, metavar=('TRAIN', 'WAGGON'),
                        help='list platform sections and exit')
    parser.add_argument('-d', '--dataset', default=None,
                        help='directory of dataset to compile, no default')
    parser.add_argument('-p', '--pickle', default=pickle,
                        help='filename of compiled dataset, default %r' % (pickle,))
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='progress indication output')
    parser.add_argument('--debug', action='store_true',
                        help='increase output level')

    args = parser.parse_args()

    level = logging.INFO
    if args.quiet:
        level = logging.ERROR
    if args.debug:
        level = logging.DEBUG

    logging.basicConfig(level=level, stream=sys.stderr)

    if args.trains is True:
        list_trains(args.dataset, args.pickle)
    elif args.section is not None:
        train, waggon = args.section
        trainNumber = decode_trainNumber(train)
        ok = list_section(trainNumber, waggon, args.dataset, args.pickle)
        if not ok:
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
