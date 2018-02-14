# Wagenreihungsplan
Using Deutsche Bahn's published data to find your platform section offline.

## Preparations

1. Download the most data set from [Deutsche Bahn](http://data.deutschebahn.com/dataset/data-wagenreihungsplan-soll-daten) and unpack as some directory containing XML files. Let us call this directory `$DB`. The dataset is currently (2018) around 80 MB.
2. Run `./wagenreihungsplan.py -t -d $DB`, where `$DB` is the data directory. This processes all XML files, writing a `.pickle.bz`-file. Processing can take a few minutes. The output is a table of all trains (on `stdout`) and some progress information (on `stderr`).
3. You can remove the downloaded directory now.

## Usage

* `./wagenreihungsplan.py -t` prints a table of all trains. Grep for your train. Let us call the number of your train `$TRAIN`. Also find out the identifier for your carriage, which we will call `$WAGGON`.
* `./wagenreihungsplan.py -s $TRAIN $WAGGON` prints the run of your train, as far as known from the data set, with the platform section where the carriage is scheduled to stop at the various stations.


