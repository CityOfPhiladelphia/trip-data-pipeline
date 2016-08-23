#!/usr/bin/env python

import click
from glob import iglob
from itertools import chain
import petl


def fromcsvs(filepatterns, fieldnames=None):
    """
    Create a table from a list of file names. Fieldnames is an iterable which,
    when specified, is pushed on as the header for the table.
    """
    t = None
    for fname in chain.from_iterable(iglob(p) for p in filepatterns):
        t_partial = petl.fromcsv(fname)
        if fieldnames is not None:
            t_partial = t_partial.pushheader(fieldnames)
        t = t_partial if t is None else t.cat(t_partial)
    return t


def asmoney(value):
    """Represent the given value as currency"""
    return '${:.2f}'.format(round(float(value), 2))


def transform(verifone_filenames, cmt_filenames):
    """
    1. Combine CMT/Verifone data
    2. Add a new column that specifies whether each trip came from CMT or
       Verifone
    3. Remove Shift # column.
    4. Remove Device Type column.
    5. Format Columns R, S, T, U, V, and W to be 4 decimal points and currency.

       TODO: ^^^^ CHECK ON THAT ONE; DOESN'T SOUND RIGHT. WHY 4 DECIMALS? ^^^^

    6. Anonymize the 'Medallion #' fields or remove that column entirely and
       instead create a column with a unique ID to represent each driver,
       based on the 'Chauffeur #' field in the CMT and Verifone files.

    Updates weekly.
    """
    t_ver = fromcsvs(verifone_filenames, fieldnames=['Shift #', 'Trip #', 'Operator Name', 'Medallion', 'Device Type', 'Chauffeur #', 'Meter On Datetime', 'Meter Off Datetime', 'Trip Length', 'Pickup Latitude', 'Pickup Longitude', 'Pickup Location', 'Dropoff Latitude', 'Dropoff Longitude', 'Dropoff Location', 'Fare', 'Tax', 'Tips', 'Tolls', 'Surcharge', 'Trip Total', 'Payment Type', 'Street/Dispatch'])\
        .addfield('Data Source', 'verifone')
    t_cmt = fromcsvs(cmt_filenames)\
        .addfield('Data Source', 'cmt')

    t = petl.cat(t_ver, t_cmt)\
        .cutout('Shift #')\
        .convert('Fare', asmoney)\
        .convert('Tax', asmoney)\
        .convert('Tips', asmoney)\
        .convert('Tolls', asmoney)\
        .convert('Surcharge', asmoney)\
        .convert('Trip Total', asmoney)

    return t


# ============================================================


@click.command()
@click.option('--verifone', '-v', type=click.Path(), multiple=True, help='Verifone data files')
@click.option('--cmt', '-c', type=click.Path(), multiple=True, help='CMT data files')
def transform_cmd(verifone, cmt):
    transform(verifone, cmt)\
        .progress()\
        .tocsv()


if __name__ == '__main__':
    transform_cmd()
