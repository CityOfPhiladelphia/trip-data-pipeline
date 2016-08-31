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

In the database, first create the taxi_trips table:

    CREATE TABLE taxi_trips (
      trip_no VARCHAR(16),
      operator_name NVARCHAR2(2000),
      medallion VARCHAR(16),
      chauffeur_no VARCHAR(16),
      meter_on_datetime VARCHAR(24),
      meter_off_datetime VARCHAR(24),
      trip_length VARCHAR(16),
      pickup_latitude VARCHAR(16),
      pickup_longitude VARCHAR(16),
      pickup_location NVARCHAR2(2000),
      dropoff_latitude VARCHAR(16),
      dropoff_longitude VARCHAR(16),
      dropoff_location NVARCHAR2(2000),
      fare VARCHAR(16),
      tax VARCHAR(16),
      tips VARCHAR(16),
      tolls VARCHAR(16),
      surcharge VARCHAR(16),
      trip_total VARCHAR(16),
      payment_type VARCHAR(16),
      street_or_dispatch VARCHAR(32),
      data_source VARCHAR(8)
    )

Also, create an index on what is a maximal unique identifier for taxi trips; we
use it for upsertig records into the trips table:

    CREATE INDEX taxi_trip_unique_id ON taxi_trips (
      Trip_No, Medallion, Chauffeur_No, Meter_On_Datetime, Meter_Off_Datetime
    )

For anonymizing chauffeur and medallion numbers, create two tables to maintain a
mapping from actual Medallion and Chauffeur numbers to arbitrary identifiers.
With Oracle 12c+, use the following SQL:

    CREATE SEQUENCE chauffeur_no_seq;
    CREATE TABLE chauffeur_no_ids (
      ID           NUMBER DEFAULT chauffeur_no_seq.NEXTVAL,
      Chauffeur_No VARCHAR2(16)
    );

    CREATE SEQUENCE medallion_seq;
    CREATE TABLE medallion_ids (
      ID        NUMBER DEFAULT medallion_seq.NEXTVAL,
      Medallion VARCHAR2(16)
    );

For Oracle pre-12c, use the following:

    CREATE TABLE chauffeur_no_ids (
      ID            NUMBER         NOT NULL,
      Chauffeur_No  VARCHAR2(16) NOT NULL);
    CREATE INDEX chauffeur_no_idx ON chauffeur_no_ids (Chauffeur_No)
    CREATE SEQUENCE chauffeur_no_seq;
    CREATE OR REPLACE TRIGGER chauffeur_no_trig
    BEFORE INSERT ON chauffeur_no_ids
    FOR EACH ROW
    BEGIN
      SELECT chauffeur_no_seq.NEXTVAL
      INTO   :new.ID
      FROM   dual;
    END;

    CREATE TABLE medallion_ids (
      ID         NUMBER         NOT NULL,
      Medallion  VARCHAR2(16) NOT NULL);
    CREATE INDEX medallion_idx ON medallion_ids (Medallion)
    CREATE SEQUENCE medallion_seq;
    CREATE OR REPLACE TRIGGER medallion_trig
    BEFORE INSERT ON medallion_ids
    FOR EACH ROW
    BEGIN
      SELECT medallion_seq.NEXTVAL
      INTO   :new.ID
      FROM   dual;
    END;

Finally, for the public, create the following view:

    CREATE VIEW anonymized_taxi_trips AS
        SELECT Trip_No, Operator_Name,
            mids.ID AS Medallion_ID,
            cnids.ID AS Chauffeur_ID,
            Meter_On_Datetime, Meter_Off_Datetime,
            Trip_Length,
            Pickup_Latitude, Pickup_Longitude, Pickup_Location,
            Dropoff_Latitude, Dropoff_Longitude, Dropoff_Location,
            Fare, Tax, Tips, Tolls, Surcharge, Trip_Total,
            Payment_Type,
            Street_or_Dispatch,
            Data_Source
        FROM taxi_trips ts
        LEFT JOIN medallion_ids mids ON ts.Medallion = mids.Medallion
        LEFT JOIN chauffeur_no_ids cnids ON ts.Chauffeur_No = cnids.Chauffeur_No;

Updates weekly.
"""

import click
from contextlib import contextmanager
from datetime import datetime
from glob import iglob
from itertools import chain
import petl

import logging
logger = logging.getLogger(__name__)


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

def asisodatetime(value):
    """Convert a date as YYYY-MM-DD HH:MM:SS.UUU"""
    try:
        return datetime\
            .strptime(value.strip(), '%m/%d/%Y %H:%M')\
            .strftime('%Y-%m-%d %H:%I:00.000')
    except ValueError:
        if value:
            logger.warn('Could not parse date: {}'.format(value))
        return value

def asnormpaytype(value):
    if value and value == 'CASH':
        return 'Cash'
    elif value and value == 'CC CARD':
        return 'Credit Card'
    else:
        return value

def transform(verifone_filenames, cmt_filenames):
    """
    1. Combine CMT/Verifone data
    2. Add a new column that specifies whether each trip came from CMT or
       Verifone
    3. Remove Shift # column.
    4. Remove Device Type column.
    5. Format Columns R, S, T, U, V, and W to be 2 decimal points and currency.
    6. Round minutes to the nearest 15 minutes.
    """
    t_ver = fromcsvs(verifone_filenames, fieldnames=['Shift #', 'Trip #', 'Operator Name', 'Medallion', 'Device Type', 'Chauffeur #', 'Meter On Datetime', 'Meter Off Datetime', 'Trip Length', 'Pickup Latitude', 'Pickup Longitude', 'Pickup Location', 'Dropoff Latitude', 'Dropoff Longitude', 'Dropoff Location', 'Fare', 'Tax', 'Tips', 'Tolls', 'Surcharge', 'Trip Total', 'Payment Type', 'Street/Dispatch'])\
        .addfield('Data Source', 'verifone')\
        .convert('Payment Type', asnormpaytype)
    t_cmt = fromcsvs(cmt_filenames)\
        .addfield('Data Source', 'cmt')\
        .convert('Meter On Datetime', asisodatetime)\
        .convert('Meter Off Datetime', asisodatetime)

    # TODO: Round minutes to nearest 15 minutes

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


def upload(table_filename, username, password, db_conn_string,
           wrap_table=lambda t: t, group_size=100000):
    """
    Load a merged taxi trips table from a CSV file into the database (first step
    in anonymization process). Only insert new data.

    The wrap_table function can be used to modify the table before passing it
    along to the upsert function. For example:

        upload(..., wrap_table=lambda t: t.progress())

    You can specify how many upsert statements are sent to the server at a time
    with the group_size keyword.
    """
    with transaction(username, password, db_conn_string) as cursor:
        t = petl.fromcsv(table_filename)\
            .setheader(['Trip_No', 'Operator_Name', 'Medallion', 'Chauffeur_No', 'Meter_On_Datetime', 'Meter_Off_Datetime', 'Trip_Length', 'Pickup_Latitude', 'Pickup_Longitude', 'Pickup_Location', 'Dropoff_Latitude', 'Dropoff_Longitude', 'Dropoff_Location', 'Fare', 'Tax', 'Tips', 'Tolls', 'Surcharge', 'Trip_Total', 'Payment_Type', 'Street_or_Dispatch', 'Data_Source'])
        todb_upsert(wrap_table(t), 'taxi_trips', cursor, group_size=group_size)

    return t


@contextmanager
def transaction(username, password, db_conn_string):
    import cx_Oracle
    conn = cx_Oracle.connect(username, password, db_conn_string)
    cursor = conn.cursor()

    yield cursor

    conn.commit()
    conn.close()


def grouper(n, iterable, fillvalue=None):
    """
    Yield groups of size n from the iterable (e.g.,
    grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx").
    Pulled from itertools recipes.
    """
    from itertools import zip_longest
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


def todb_upsert(table, table_name, cursor, group_size=1000):
    # Create a list of the column names
    columns = table.fieldnames()
    id_columns = {'Trip_No', 'Medallion', 'Chauffeur_No', 'Meter_On_Datetime', 'Meter_Off_Datetime'}
    non_id_columns = set(columns) - id_columns

    # Build the clauses for the SQL statement
    select_clause = 'SELECT {} FROM DUAL'.format(
        ', '.join(':{0} AS {0}'.format(c) for c in columns))
    on_clause = ' AND '.join('orig.{0} = new.{0}'.format(c) for c in id_columns)
    update_clause = 'UPDATE SET {}'.format(
        ', '.join('orig.{0} = new.{0}'.format(c) for c in non_id_columns))
    insert_clause = 'INSERT ({}) VALUES ({})'.format(
        ', '.join('orig.{}'.format(c) for c in columns),
        ', '.join('new.{}'.format(c) for c in columns))

    sql = '''
    MERGE INTO {} orig
        USING ({}) new
        ON ({})
        WHEN MATCHED THEN {}
        WHEN NOT MATCHED THEN {}
    '''.format(table_name, select_clause, on_clause, update_clause, insert_clause)

    for row_group in grouper(group_size, table.values(columns)):
        row_group = filter(None, row_group)
        list_of_rows = list(row_group)
        cursor.executemany(sql, list_of_rows)
    cursor.connection.commit()


def anonymize(username, password, dsn, table_name, column_table_pairs):
    """
    Create unique identifiers for chauffeurs and medallions in the taxi_trips
    table.

    The anonymization tables are listed, one for each column to be anonymized,
    in column_table_pairs, which is a set of 2-tuples of the form:

        [(column_name_1, ids_table_name_1),
         (column_name_2, ids_table_name_2),
         ...]

    The ID tables should be created with a column matching the column name from
    the original table, and an auto-incrementing ID column. See above for how
    the id lookup table should be created.
    """
    make_sql = lambda column_name, ids_table_name: '''
        MERGE INTO {ids_table_name} orig
            USING (
                SELECT ts.{column_name}
                    FROM {table_name} ts
                    LEFT JOIN {ids_table_name} ids
                    ON ts.{column_name} = ids.{column_name}
                    WHERE ids.id IS NULL
                    AND ts.{column_name} IS NOT NULL
                    GROUP BY ts.{column_name}
                ) new
            ON (orig.{column_name} = new.{column_name})
            WHEN NOT MATCHED THEN
                INSERT (orig.{column_name}) VALUES (new.{column_name})
        '''.format(table_name=table_name,
                   column_name=column_name,
                   ids_table_name=ids_table_name)

    with transaction(username, password, dsn) as cursor:
        for col, ids in column_table_pairs:
            sql = make_sql(col, ids)
            cursor.execute(sql)



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
    upload(csv_filename, username, password, dsn, wrap_table=lambda t: t.progress())


@cli.command(name='anonymize')
@click.option('--username', '-u', help='The database username')
@click.option('--password', '-p', help='The database user password')
@click.option('--dsn', '-d', help='The database DSN connection script')
def anonymize_cmd(username, password, dsn):
    anonymize(username, password, dsn, 'taxi_trips', [
        ('Chauffeur_No', 'chauffeur_no_ids'),
        ('Medallion', 'medallion_ids'),
    ])


if __name__ == '__main__':
    cli()
