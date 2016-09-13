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


@click.group()
def cli():
    pass


@cli.command(name='transform')
@click.option('--verifone', '-v', type=click.Path(), multiple=True, help='Verifone data files')
@click.option('--cmt', '-c', type=click.Path(), multiple=True, help='CMT data files')
def transform_cmd(verifone, cmt):
    transform(verifone, cmt)\
        .progress()\
        .tocsv()


@cli.command(name='upload')
@click.option('--database', '-d', help='The database connection string')
@click.argument('csv_filename')
def upload_cmd(csv_filename, database):
    upload(csv_filename, database, wrap_table=lambda t: t.progress())


@cli.command(name='anonymize')
@click.option('--database', '-d', help='The database connection string')
def anonymize_cmd(database):
    anonymize(database, 'taxi_trips', [
        ('Chauffeur_No', 'chauffeur_no_ids'),
        ('Medallion', 'medallion_ids'),
    ])


if __name__ == '__main__':
    cli()
