Taxi Trip Data
==============

Scripts to clean up

## Installation

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

# Save result to a file:
taxitrips.py transform -v "testdata/verifone*" -c "testdata/cmt*" > testdata/merged.csv

# Send result to standard output:
taxitrips.py transform -v "testdata/verifone*" -c "testdata/cmt*" 2> /dev/null

# Upsert the generated data into an Oracle database
taxitrips.py upload testdata/merged.scv -u <username> -p <password> -d <dsn>

# Update the anonymization tables
taxitrips.py anonymize -u <username> -p <password> -d <dsn>
```