#!/usr/bin/env python2.7
# -*- mode: python; coding: us-ascii; indent-tabs-mode: nil -*-
# vim: set filetype=python fileencoding=utf-8 expandtab sw=4 sts=4:
#
# Bahn Wagenreihung (aka. Wagenstandsanzeiger).
# SE, created 20-Jun-2016, in Python 2.7.11.
#
# References:
# [1] http://data.deutschebahn.com/dataset/data-wagenreihungsplan-soll-daten

"""
Using Deutsche Bahn's published data to find your platform section offline.

Try:
./wagenreihungsplan.py -t
./wagenreihungsplan.py -s 141 8
"""

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
    "Report timestamp (UTC), then relative to first."

    global _FIRST_TIMESTAMP # pylint: disable=W0603
    now = datetime.datetime.utcnow()
    if _FIRST_TIMESTAMP is None:
        _FIRST_TIMESTAMP = now
        return 'T=%s' % now.strftime('%Y%m%dT%H%M%SZ')
    else:
        delta = (now - _FIRST_TIMESTAMP).total_seconds()
        return 'T + %.1fs' % delta

# -- Utilities for XML --

def text(node):
    "Get all text inside a node."
    if isinstance(node, list):
        return ''.join([text(x) for x in node])
    elif isinstance(node, xml.dom.minidom.Text):
        assert isinstance(node.data, basestring)
        return node.data
    elif isinstance(node, xml.dom.minidom.Element):
        return text(node.childNodes)
    else:
        raise Exception('Unrecognized type of object %r' % (node,))

def text_by_tag(node, tag, sep=None):
    "Get text from child nodes, joined with separator."
    children = [text(c) for c in node.childNodes
                if isinstance(c, xml.dom.minidom.Element)
                and c.tagName == tag]
    assert len(children) >= 1, (node, tag)
    assert len(children) == 1 or sep is not None, (node, tag, sep)
    if sep is None:
        return children[0]
    else:
        return sep.join(children)


# -- Decoding XML from 'Wagenreihungsplan_RawData_*/*.xml' --

def read_train_numbers(datasetdir):
    "Read all XML data files and yield trainNumber elements."
    infiles = sorted(os.listdir(datasetdir))
    # infiles = ['BLS_2017-11-24_12-28-07.xml', 'MH_2017-12-01_09-31-22.xml']
    for infile in infiles:
        if infile.endswith('.xml'):
            inpath = os.path.abspath(os.path.join(datasetdir, infile))
            logging.info('Reading %r.', os.path.relpath(inpath, datasetdir))
            dom = xml.dom.minidom.parse(os.path.join(datasetdir, infile))
            for elt in dom.getElementsByTagName('trainNumber'):
                yield elt

_INT_RE = re.compile('^[0-9]+$')

def decode_train_number(obj):
    "Decode a trainNumber into an int|str."
    string = str(obj).strip().lower()
    if _INT_RE.match(string):
        return int(string)
    else:
        return string

def read_dataset(datasetdir):
    "Read the entire dataset."
    for train_number in read_train_numbers(datasetdir):
        train_train_number = decode_train_number(text(train_number))

        train = train_number.parentNode.parentNode
        assert train.tagName == 'train'
        train_name = text_by_tag(train, 'name')
        train_time = text_by_tag(train, 'time')

        track = train.parentNode.parentNode
        assert track.tagName == 'track'
        track_name = text_by_tag(track, 'name')

        station = track.parentNode.parentNode
        assert station.tagName == 'station'
        station_shortcode = text_by_tag(station, 'shortcode')
        station_name = text_by_tag(station, 'name')

        for waggon in train.getElementsByTagName('waggon'):
            waggon_number = text_by_tag(waggon, 'number').lower()

            for section in waggon.getElementsByTagName('sections'):
                section_identifiers = text_by_tag(
                    section, 'identifier', sep='/')
                yield {'train.trainNumber': train_train_number,
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

    dataset = {}
    dataset['$type'] = _TYPE
    dataset['$dataset'] = datasetpath
    dataset['entries'] = sorted(list(read_dataset(datasetpath)))
    return dataset

_TYPE = 'Wagenreihungsplan' # The tag to identify our datasets.

def load_dataset(datasetdir, picklefile):
    "Load dataset from picklefile, or recreate it from datasetdir."
    picklepath = os.path.abspath(picklefile)
    datasetpath = None
    if datasetdir is not None:
        datasetpath = os.path.abspath(datasetdir)

    dataset = None
    if os.path.isfile(picklepath):
        logging.debug(
            'Loading compiled dataset %r at %s.', picklepath, timestamp())
        with bz2.BZ2File(picklepath, 'rb') as inp:
            dataset = cPickle.load(inp)

    is_valid = (
        isinstance(dataset, dict) and
        dataset.has_key('$type') and dataset['$type'] == _TYPE and
        dataset.has_key('$dataset') and
        (datasetpath is None or dataset['$dataset'] == datasetpath))
    if not is_valid:

        dataset = compile_dataset(datasetpath)

        pickledir = os.path.dirname(picklepath)
        if not os.path.isdir(pickledir):
            logging.info('Creating directory %r.', pickledir)
            os.makedirs(pickledir)

        logging.info(
            'Saving compiled dataset %r, at %s.', picklepath, timestamp())
        with bz2.BZ2File(picklepath, 'wb') as out:
            cPickle.dump(dataset, out)

    logging.debug('Dataset loaded at %s.', timestamp())
    return dataset


# -- Table output --

def field_widths(table):
    "Determine the width of the columns."
    fws = []
    for row in table:
        for i in range(len(row)):
            if not i < len(fws):
                fws.append(0)
            fws[i] = max(fws[i], len(row[i]))
    return fws

def print_table(table):
    "Print a formatted table, all fields aligned left."
    fws = field_widths(table)
    for row in table:
        for i in range(len(fws)):
            field = ''
            if i < len(row):
                field = row[i]
            padding = fws[i] - len(field) + 2
            out = codecs.encode(field + ' '*padding, 'UTF-8')
            sys.stdout.write(out)
        sys.stdout.write('\n')


# -- Use compiled dataset --

_INVALID_TRAIN_TIME = '99:99:00'

def list_trains(datasetdir, picklefile):
    "Print the table of trains."
    dataset = load_dataset(datasetdir, picklefile)

    tns = set()
    first_by_tn = {}
    last_by_tn = {}
    for entry in dataset['entries']:
        num = entry['train.trainNumber']
        tim = entry['train.time']
        if tim != _INVALID_TRAIN_TIME:
            if num in tns:
                if tim < first_by_tn[num]['train.time']:
                    first_by_tn[num] = entry
                if tim > last_by_tn[num]['train.time']:
                    last_by_tn[num] = entry
            else:
                first_by_tn[num] = entry
                last_by_tn[num] = entry
            tns.add(num)

    table = [['Zug', 'Zugname', 'Ab', 'Von', 'An', 'Nach']]
    for num in sorted(list(tns)):
        first = first_by_tn[num]
        last = last_by_tn[num]

        row = []
        row.append(str(num))
        row.append(first['train.name'])
        row.append(first['train.time'])
        row.append('%s (%s)' % (
            first['station.name'], first['station.shortcode']))
        row.append(last['train.time'])
        row.append('%s (%s)' % (
            last['station.name'], last['station.shortcode']))
        table.append(row)

    print_table(table)

def list_section(train_number, waggon, datasetdir, picklefile):
    "Print the table of sections."
    dataset = load_dataset(datasetdir, picklefile)
    es1 = [e for e in dataset['entries']
           if e['train.trainNumber'] == train_number and
           e['train.time'] != _INVALID_TRAIN_TIME]
    if len(es1) == 0:
        print 'Zug %r nicht gefunden.' % (train_number,)
        return False
    es2 = [e for e in es1 if e['waggon.number'] == waggon]
    if len(es2) == 0:
        print 'Wagen %r von Zug %r nicht gefunden.' % (
            waggon, train_number)
        return False

    es3 = sorted([(e['train.time'], e) for e in es2])
    es4 = [e for (_, e) in es3]

    table = [['Ab', 'Von', 'Gleis', 'Abschnitt']]
    for entry in es4:
        row = []
        row.append(entry['train.time'])
        row.append('%s (%s)' % (
            entry['station.name'], entry['station.shortcode']))
        row.append(entry['track.name'])
        row.append(entry['section.identifiers'])
        table.append(row)

    print_table(table)
    return True

_DEFAULT_PICKLE = 'Wagenreihungsplan.pickle.bz2'

def main():
    "Run the application."

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-t', '--trains', action='store_true',
        help='list trains and exit')
    parser.add_argument(
        '-s', '--section', nargs=2, metavar=('TRAIN', 'WAGGON'),
        help='list platform sections and exit')
    parser.add_argument(
        '-d', '--dataset', default=None,
        help='directory of dataset to compile, no default')
    parser.add_argument(
        '-p', '--pickle', default=_DEFAULT_PICKLE,
        help='filename of compiled dataset, default %r' % (_DEFAULT_PICKLE,))
    parser.add_argument(
        '-q', '--quiet', action='store_true',
        help='progress indication output')
    parser.add_argument(
        '--debug', action='store_true',
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
        train_number = decode_train_number(train)
        success = list_section(
            train_number, waggon, args.dataset, args.pickle)
        if not success:
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
