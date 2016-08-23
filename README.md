Taxi Trip Data
==============

## Installation

pip install https://github.com/CityOfPhiladelphia/taxi-trip-data-pipeline.git#egg=taxi-trip-data-pipeline


## Usage

```bash
python taxitrips.py --help
# Usage: taxitrips.py [OPTIONS]
#
# Options:
#   -v, --verifone PATH  Verifone data files
#   -c, --cmt PATH       CMT data files
#   --help               Show this message and exit.
```

**Example:**

```bash
# Save result to a file:
taxitrips.py -v "testdata/verifone*" -c "testdata/cmt*" > output/merged.csv

# Send result to standard output:
taxitrips.py -v "testdata/verifone*" -c "testdata/cmt*" 2> /dev/null
```