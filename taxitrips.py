#!/usr/bin/env python

"""
Script to perform the following steps for taxi trip data:

1. Combine CMT/Verifone data
2. Add a new column that specifies whether each trip came from CMT or
   Verifone
3. Remove Shift # column.
4. Remove Device Type column.
5. Format Columns R, S, T, U, V, and W to be 2 decimal points and currency.
6. Anonymize the 'Medallion #' fields or remove that column entirely and
   instead create a column with a unique ID to represent each driver,
   based on the 'Chauffeur #' field in the CMT and Verifone files.

Updates weekly.
"""

import click
from phila_taxitrips import (normalize, upload, update_anon, anonymize, fuzzy,
    validate_trip_lengths,
    RAW_COLUMNS_CSV, RAW_COLUMNS_DB, PUBLIC_COLUMNS_CSV, PUBLIC_COLUMNS_DB)
import sys


@click.group()
def cli():
    pass


@cli.command(name='normalize')
@click.option('--verifone', '-v', type=click.Path(), multiple=True, help='Verifone data files')
@click.option('--cmt', '-c', type=click.Path(), multiple=True, help='CMT data files')
def normalize_cmd(verifone, cmt):
    normalize(verifone, cmt)\
        .progress()\
        .tocsv()


@cli.command(name='uploadraw')
@click.option('--database', '-d', help='The database connection string')
@click.argument('csvfile', type=click.Path())
def uploadraw_cmd(csvfile, database):
    upload(csvfile, database, 'taxi_trips', RAW_COLUMNS_CSV, RAW_COLUMNS_DB, wrap_table=lambda t: t.progress())
    update_anon(database, 'taxi_trips', [
        ('Chauffeur_No', 'chauffeur_no_ids'),
        ('Medallion', 'medallion_ids'),
    ])


@cli.command(name='anonymize')
@click.option('--database', '-d', help='The database connection string')
@click.option('--log', '-l', help='Log level. Default is debug')
@click.argument('csvfile', type=click.Path())
def anonymize_cmd(csvfile, database, log):
    if log:
        import logging
        logging.basicConfig(level=getattr(logging, log.upper()))
    anonymize(csvfile, database, [
        ('Chauffeur #', 'chauffeur_no_ids', 'Chauffeur_No'),
        ('Medallion', 'medallion_ids', 'Medallion'),
    ])\
        .progress(10000)\
        .tocsv()

@cli.command(name='fuzzy')
@click.argument('csvfile', type=click.Path())
@click.option('--regions', '-r', type=click.File('r'), help='Shapes to be used for binning trips inside of the City')
def fuzzy_cmd(csvfile, regions):
    fuzzy(csvfile, regions)\
        .progress()\
        .tocsv()

@cli.command(name='validate')
@click.argument('csvfile', type=click.Path())
def validate_cmd(csvfile):
    table, errors = validate_trip_lengths(csvfile)
    table.tocsv()
    print('\n'.join(errors), file=sys.stderr)
    sys.exit(1 if errors else 0)

@cli.command(name='uploadpublic')
@click.option('--database', '-d', help='The database connection string')
@click.argument('csvfile', type=click.Path())
def uploadpublic_cmd(csvfile, database):
    upload(csvfile, database, 'public_taxi_trips', PUBLIC_COLUMNS_CSV, PUBLIC_COLUMNS_DB, wrap_table=lambda t: t.progress(10000))\
        .tocsv()


if __name__ == '__main__':
    cli()
