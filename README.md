Taxi Trip Data
==============

Scripts to clean up

## Installation

Requires libspatialindex for fuzzy-ing locations.

sudo apt-get install libspatialindex-dev

pip install https://github.com/CityOfPhiladelphia/taxi-trip-data-pipeline.git#egg=taxi-trip-data-pipeline


### Development

pip install -e .


## Usage

```bash
taxitrips.py --help
# Usage: taxitrips.py [OPTIONS] COMMAND [ARGS]...
#
# Options:
#   --help  Show this message and exit.
#
# Commands:
#   anonymize
#   transform
#   upload

# EXAMPLES:

# (1) Save result to a file:
taxitrips.py normalize -v "testdata/verifone*" -c "testdata/cmt*" > testdata/merged.csv

# (2) Upsert the generated data into an Oracle database
taxitrips.py uploadraw testdata/merged.csv -d <db_conn_str>

# (3) Update the anonymization tables
taxitrips.py anonymize testdata/merged.csv -d <db_conn_str> > testdata/anonymized.csv

# (4) Fuzzy the locations and times
taxitrips.py fuzzy testdata/anonymized.csv > testdata/fuzzied.csv

# (5) Upsert the public data table in to Oracle
taxitrips.py uploadpublic testdata/fuzzied.csv -d <db_conn_str>
```

## Notes

* A full year of data could have around 8,000,000 data points. Step (1) above
  may take about 20 minutes to merge and transform this data. Step (2) may take
  another 10 minutes.