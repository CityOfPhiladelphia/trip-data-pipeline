from contextlib import contextmanager
from datetime import datetime
import datum
import os
import phltaxitrips.petl_ext as petl
from phltaxitrips.petl_ext import asnormpaytype, asisodatetime, asmoney


# Prevent cx_Oracle from converting everything to ASCII.
os.environ['NLS_LANG'] = '.UTF8'

import logging
logger = logging.getLogger(__name__)


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
    # Load and normalize Verifone tables
    ver_table = petl.fromcsvs(verifone_filenames, fieldnames=['Shift #', 'Trip #', 'Operator Name', 'Medallion', 'Device Type', 'Chauffeur #', 'Meter On Datetime', 'Meter Off Datetime', 'Trip Length', 'Pickup Latitude', 'Pickup Longitude', 'Pickup Location', 'Dropoff Latitude', 'Dropoff Longitude', 'Dropoff Location', 'Fare', 'Tax', 'Tips', 'Tolls', 'Surcharge', 'Trip Total', 'Payment Type', 'Street/Dispatch'])\
        .addfield('Data Source', 'verifone')\
        .convert('Payment Type', asnormpaytype)

    # Load and normalize CMT tables
    cmt_table = petl.fromcsvs(cmt_filenames)\
        .addfield('Data Source', 'cmt')\
        .convert('Meter On Datetime', asisodatetime)\
        .convert('Meter Off Datetime', asisodatetime)

    # Concatenate verifone and cmt tables, and do further transformations
    concat_table = petl.cat(ver_table, cmt_table)\
        .cutout('Trip #')\
        .cutout('Shift #')\
        .cutout('Device Type')\
        .cutout('Pickup Location')\
        .cutout('Dropoff Location')\
        .convert('Fare', asmoney)\
        .convert('Tax', asmoney)\
        .convert('Tips', asmoney)\
        .convert('Tolls', asmoney)\
        .convert('Surcharge', asmoney)\
        .convert('Trip Total', asmoney)\

    # Additional data suggested by Tom Swanson
    dt_pattern = '%Y-%m-%d %H:%M:%S.%f'
    def parse_date(field):
        def parse_date_from_row(row):
            try:
                return datetime.strptime(row[field], dt_pattern)
            except ValueError:
                return datetime(1970, 1, 1)
        return parse_date_from_row

    concat_table = concat_table\
        .addfield('_pickup_dt', parse_date('Meter On Datetime'))\
        .addfield('_dropoff_dt', parse_date('Meter Off Datetime'))\
        .addfield('Pickup Year', lambda row: row._pickup_dt.year)\
        .addfield('Pickup Month', lambda row: row._pickup_dt.month)\
        .addfield('Pickup Day', lambda row: row._pickup_dt.day)\
        .addfield('Pickup Hour', lambda row: row._pickup_dt.hour)\
        .addfield('Pickup DOW', lambda row: row._pickup_dt.strftime('%w'))\
        .addfield('Pickup Day of Week', lambda row: row._pickup_dt.strftime('%A'))\
        .addfield('Dropoff Year', lambda row: row._dropoff_dt.year)\
        .addfield('Dropoff Month', lambda row: row._dropoff_dt.month)\
        .addfield('Dropoff Day', lambda row: row._dropoff_dt.day)\
        .addfield('Dropoff Hour', lambda row: row._dropoff_dt.hour)\
        .addfield('Dropoff DOW', lambda row: row._dropoff_dt.strftime('%w'))\
        .addfield('Dropoff Day of Week', lambda row: row._dropoff_dt.strftime('%A'))\
        .addfield('Trip Duration (minutes)', lambda row: int((row._dropoff_dt - row._pickup_dt).total_seconds() / 60), index=5)\
        .cutout('_pickup_dt', '_dropoff_dt')

    return concat_table


def upload(table_filename, db_conn_string,
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
    with db_conn(db_conn_string) as db:
        t = petl.fromcsv(table_filename)\
            .setheader(['Trip_No', 'Operator_Name', 'Medallion', 'Chauffeur_No', 'Meter_On_Datetime', 'Meter_Off_Datetime', 'Trip_Length', 'Pickup_Latitude', 'Pickup_Longitude', 'Pickup_Location', 'Dropoff_Latitude', 'Dropoff_Longitude', 'Dropoff_Location', 'Fare', 'Tax', 'Tips', 'Tolls', 'Surcharge', 'Trip_Total', 'Payment_Type', 'Street_or_Dispatch', 'Data_Source', 'Geom'])
        petl.todb_upsert(wrap_table(t), 'taxi_trips', db, group_size=group_size)

    return t


@contextmanager
def db_conn(db_conn_string):
    db = datum.connection(db_conn_string)
    yield db
    db.save()
    db.close()


def anonymize(db_conn_str, table_name, column_table_pairs):
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

    with db_conn(db_conn_str) as db:
        for col, ids in column_table_pairs:
            sql = make_sql(col, ids)
            db.execute(sql)
