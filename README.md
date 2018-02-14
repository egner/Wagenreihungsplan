# Wagenreihungsplan
Using Deutsche Bahn's published data to find your platform section offline.

## Preparations

1. Download the most data set from [Deutsche Bahn](http://data.deutschebahn.com/dataset/data-wagenreihungsplan-soll-daten) and unpack as some directory containing XML files. Let us call this directory `$DB`. The dataset is currently (2018) around 80 MB.
2. Run `./wagenreihungsplan.py -t -d $DB`, where `$DB` is the data directory. This processes all XML files, writing a `.pickle.bz`-file. This takes a few minutes. The output is a table of all trains (on `stdout`) and some progress information (on `stderr`).

## Usage

3. Run `./wagenreihungsplan.py -t` to print the list of all trains again, if you want. Grep for your train. Let us call the number of your train `$TRAIN`. Also find out the identifier for your carriage, which we will call `$WAGGON`.
4. Run `./wagenreihungsplan.py -s $TRAIN $WAGGON` to print a table of your train, as far as known from the data set, with the platform section where the carriage will stop.



