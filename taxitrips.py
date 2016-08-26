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
    5. Format Columns R, S, T, U, V, and W to be 2 decimal points and currency.
    """
    t_ver = fromcsvs(verifone_filenames, fieldnames=['Shift #', 'Trip #', 'Operator Name', 'Medallion', 'Device Type', 'Chauffeur #', 'Meter On Datetime', 'Meter Off Datetime', 'Trip Length', 'Pickup Latitude', 'Pickup Longitude', 'Pickup Location', 'Dropoff Latitude', 'Dropoff Longitude', 'Dropoff Location', 'Fare', 'Tax', 'Tips', 'Tolls', 'Surcharge', 'Trip Total', 'Payment Type', 'Street/Dispatch'])\
        .addfield('Data Source', 'verifone')
    t_cmt = fromcsvs(cmt_filenames)\
        .addfield('Data Source', 'cmt')

    t = petl.cat(t_ver, t_cmt)\
        .cutout('Shift #')\
        .cutout('Device Type')\
        .convert('Fare', asmoney)\
        .convert('Tax', asmoney)\
        .convert('Tips', asmoney)\
        .convert('Tolls', asmoney)\
        .convert('Surcharge', asmoney)\
        .convert('Trip Total', asmoney)

    return t


def upload(table_filename, username, password, db_conn_string, t_func=lambda t: t):
    """
    Load a merged taxi trips table from a CSV file into the database (first step
    in anonymization process). Only insert new data.
    """
    import cx_Oracle
    conn = cx_Oracle.connect(username, password, db_conn_string)
    cursor = conn.cursor()

    t = petl.fromcsv(table_filename)\
        .setheader(['Trip_No', 'Operator_Name', 'Medallion', 'Chauffeur_No', 'Meter_On_Datetime', 'Meter_Off_Datetime', 'Trip_Length', 'Pickup_Latitude', 'Pickup_Longitude', 'Pickup_Location', 'Dropoff_Latitude', 'Dropoff_Longitude', 'Dropoff_Location', 'Fare', 'Tax', 'Tips', 'Tolls', 'Surcharge', 'Trip_Total', 'Payment_Type', 'Street_or_Dispatch', 'Data_Source'])
    todb_upsert(t_func(t), cursor)
    conn.commit()
    conn.close()

    return t


def grouper(n, iterable, fillvalue=None):
    """
    Yield groups of size n from the iterable (e.g.,
    grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx").
    Pulled from itertools recipes.
    """
    from itertools import zip_longest
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


def todb_upsert(table, cursor):
    # Create a list of the column names
    columns = table.fieldnames()
    id_columns = {'Trip_No', 'Medallion', 'Chauffeur_No', 'Meter_On_Datetime', 'Meter_Off_Datetime'}
    non_id_columns = set(columns) - id_columns

    # Build the clauses for the SQL statement
    select_clause = 'SELECT {} FROM DUAL'.format(
        ', '.join(':{0} AS {0}'.format(c) for c in columns))
    on_clause = ', '.join('orig.{0} = new.{0}'.format(c) for c in id_columns)
    update_clause = 'UPDATE SET {}'.format(
        ', '.join('orig.{0} = new.{0}'.format(c) for c in non_id_columns))
    insert_clause = 'INSERT ({}) VALUES ({})'.format(
        ', '.join('orig.{}'.format(c) for c in columns),
        ', '.join('new.{}'.format(c) for c in columns))

    sql = '''
    MERGE INTO taxi_trips orig
        USING ({}) new
        ON ({})
        WHEN MATCHED THEN {}
        WHEN NOT MATCHED THEN {}
    '''.format(select_clause, on_clause, update_clause, insert_clause)

    for row_group in grouper(100, table.values(columns)):
        row_group = filter(None, row_group)
        cursor.executemany(sql, list(row_group))



# ============================================================


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
@click.option('--username', '-u', help='The database username')
@click.option('--password', '-p', help='The database user password')
@click.option('--dsn', '-d', help='The database DSN connection script')
@click.argument('csv_filename')
def upload_cmd(csv_filename, username, password, dsn):
    upload(csv_filename, username, password, dsn, t_func=lambda t: t.progress())


if __name__ == '__main__':
    cli()
