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

For anonymizing chauffeur medallion numbers, create the following tables:

    CREATE SEQUENCE chauffeur_no_seq;
    CREATE TABLE chauffeur_no_ids (
      ID           NUMBER DEFAULT chauffeur_no_seq.NEXTVAL,
      Chauffeur_No VARCHAR2(4000)
    );

    CREATE SEQUENCE medallion_seq;
    CREATE TABLE medallion_ids (
      ID        NUMBER DEFAULT medallion_seq.NEXTVAL,
      Medallion VARCHAR2(4000)
    );

Finally, for the public, create the following view:

    CREATE VIEW anonymized_taxi_trips AS
        SELECT Trip_No, Operator_Name,
            medallion_ids.ID AS Medallion_ID,
            chauffeur_no_ids.ID AS Chauffeur_ID,
            Meter_On_Datetime, Meter_Off_Datetime,
            Trip_Length,
            Pickup_Latitude, Pickup_Longitude, Pickup_Location,
            Dropoff_Latitude, Dropoff_Longitude, Dropoff_Location,
            Fare, Tax, Tips, Tolls, Surcharge, Trip_Total,
            Payment_Type,
            Street_or_Dispatch,
            Data_Source,
            ST_GeomFromText('MULTIPOINT(' || Pickup_Longitude || ' ' || Pickup_Latitude || ', '
                                          || Dropoff_Longitude || ' ' || Dropoff_Latitude || ')',
                            4326) AS geom
        FROM taxi_trips ts
        LEFT JOIN medallion_ids mids ON ts.Medallion = mids.Medallion
        LEFT JOIN chauffeur_no_ids cnids ON ts.Chauffeur_No = cnids.Chauffeur_No;

Updates weekly.
"""

import click
from contextlib import contextmanager
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


def upload(table_filename, username, password, db_conn_string,
           wrap_table=lambda t: t, group_size=1000):
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


def todb_upsert(table, tablename, cursor, group_size=1000):
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
    MERGE INTO {} orig
        USING ({}) new
        ON ({})
        WHEN MATCHED THEN {}
        WHEN NOT MATCHED THEN {}
    '''.format(table_name, select_clause, on_clause, update_clause, insert_clause)

    for row_group in grouper(group_size, table.values(columns)):
        row_group = filter(None, row_group)
        cursor.executemany(sql, list(row_group))


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
        INSERT INTO {ids_table_name} ({column_name})
            SELECT {column_name}
                FROM {table_name} ts
                LEFT JOIN {ids_table_name} ids
                ON ts.{column_name} = ids.{column_name}
                WHERE ids.id IS NULL
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
        ('Chauffeur_No', 'taxi_chauffeur_ids'),
        ('Medallion_No', 'taxi_medallion_ids'),
    ])


if __name__ == '__main__':
    cli()
