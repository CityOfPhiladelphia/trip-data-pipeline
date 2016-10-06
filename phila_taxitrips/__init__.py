from contextlib import contextmanager
from datetime import datetime
import datum
import os
import phila_taxitrips.petl_ext as petl
from phila_taxitrips.petl_ext import asnormpaytype, asisodatetime, asmoney
import re


# Prevent cx_Oracle from converting everything to ASCII.
os.environ['NLS_LANG'] = '.UTF8'

import logging
logger = logging.getLogger(__name__)

RAW_COLUMNS_CSV = ['Operator Name', 'Medallion', 'Chauffeur #',  'Meter On Datetime', 'Meter Off Datetime', 'Trip Length', 'Pickup Latitude', 'Pickup Longitude', 'Pickup Location', 'Dropoff Latitude', 'Dropoff Longitude', 'Dropoff Location', 'Fare', 'Tax', 'Tips', 'Tolls', 'Surcharge', 'Trip Total', 'Payment Type', 'Street/Dispatch',    'Data Source']
RAW_COLUMNS_DB  = ['Operator_Name', 'Medallion', 'Chauffeur_No', 'Meter_On_Datetime', 'Meter_Off_Datetime', 'Trip_Length', 'Pickup_Latitude', 'Pickup_Longitude', 'Pickup_Location', 'Dropoff_Latitude', 'Dropoff_Longitude', 'Dropoff_Location', 'Fare', 'Tax', 'Tips', 'Tolls', 'Surcharge', 'Trip_Total', 'Payment_Type', 'Street_or_Dispatch', 'Data_Source']
PUBLIC_COLUMNS_CSV = ['Operator Name', 'Anonymized Medallion', 'Anonymized Chauffeur #',  'Pickup General Time', 'Dropoff General Time', 'Trip Length', 'Pickup Zip Code', 'Pickup Region Centroid Latitude', 'Pickup Region Centroid Longitude', 'Pickup Region ID', 'Dropoff Zip Code', 'Dropoff Region Centroid Latitude', 'Dropoff Region Centroid Longitude', 'Dropoff Region ID', 'Region Map Version', 'Fare', 'Tax', 'Tips', 'Tolls', 'Surcharge', 'Trip Total', 'Payment Type', 'Street/Dispatch',    'Data Source']
PUBLIC_COLUMNS_DB  = ['Operator Name', 'Anonymized_Medallion', 'Anonymized_Chauffeur_No', 'Pickup_General_Time', 'Dropoff_General_Time', 'Trip_Length', 'Pickup_Zip_Code', 'Pickup_Region_Centroid_Latitude', 'Pickup_Region_Centroid_Longitude', 'Pickup_Region_ID', 'Dropoff_Zip_Code', 'Dropoff_Region_Centroid_Latitude', 'Dropoff_Region_Centroid_Longitude', 'Dropoff_Region_ID', 'Region_Map_Version', 'Fare', 'Tax', 'Tips', 'Tolls', 'Surcharge', 'Trip_Total', 'Payment_Type', 'Street_or_Dispatch', 'Data_Source']

def normalize(verifone_filenames, cmt_filenames):
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
        .convert('Payment Type', asnormpaytype)\
        .convert('Meter On Datetime', lambda val: val[:19] if val else '')\
        .convert('Meter Off Datetime', lambda val: val[:19] if val else '')

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
        .convert('Fare', asmoney)\
        .convert('Tax', asmoney)\
        .convert('Tips', asmoney)\
        .convert('Tolls', asmoney)\
        .convert('Surcharge', asmoney)\
        .convert('Trip Total', asmoney)\

    # Additional data suggested by Tom Swanson
    dt_pattern = '%Y-%m-%d %H:%M:%S'
    concat_table = concat_table\
        .addfields(
            ('_pickup_dt', petl.parsedate('Meter On Datetime', dt_pattern)),
            ('_dropoff_dt', petl.parsedate('Meter Off Datetime', dt_pattern)))\
        .addfields(
            ('Pickup Year', lambda row: row._pickup_dt.year if row._pickup_dt else None),
            ('Pickup Month', lambda row: row._pickup_dt.month if row._pickup_dt else None),
            ('Pickup Day', lambda row: row._pickup_dt.day if row._pickup_dt else None),
            ('Pickup Hour', lambda row: row._pickup_dt.hour if row._pickup_dt else None),
            ('Pickup DOW', lambda row: row._pickup_dt.strftime('%w') if row._pickup_dt else None),
            ('Pickup Day of Week', lambda row: row._pickup_dt.strftime('%A') if row._pickup_dt else None),
            ('Pickup General Time', lambda row: row._pickup_dt.replace(minute=0, second=0) if row._pickup_dt else None),
            ('Dropoff Year', lambda row: row._dropoff_dt.year if row._dropoff_dt else None),
            ('Dropoff Month', lambda row: row._dropoff_dt.month if row._dropoff_dt else None),
            ('Dropoff Day', lambda row: row._dropoff_dt.day if row._dropoff_dt else None),
            ('Dropoff Hour', lambda row: row._dropoff_dt.hour if row._dropoff_dt else None),
            ('Dropoff DOW', lambda row: row._dropoff_dt.strftime('%w') if row._dropoff_dt else None),
            ('Dropoff Day of Week', lambda row: row._dropoff_dt.strftime('%A') if row._dropoff_dt else None),
            ('Dropoff General Time', lambda row: row._dropoff_dt.replace(minute=0, second=0) if row._dropoff_dt else None))\
        .addfield('Trip Duration (minutes)', lambda row: int((row._dropoff_dt - row._pickup_dt).total_seconds() / 60) if row._pickup_dt and row._dropoff_dt else None, index=5)\
        .cutout('_pickup_dt', '_dropoff_dt')

    return concat_table


def load_shapes(geojson_file):
    from shapely.geometry import shape
    from rtree import index
    import json

    # load the initial collection from the geojson
    collection = json.load(geojson_file)

    # create a spatial index
    idx = index.Index()

    for feature in collection['features']:
        # add the shapely-parsed shape onto each feature
        feature['shape'] = shape(feature['geometry'])
        feature['centroid'] = feature['shape'].centroid
        # spatially index each feature
        objid = feature['properties']['OBJECTID']
        idx.insert(objid, feature['shape'].bounds, obj=feature)

    return collection, idx


def feature_property(latcol, lngcol, collection, property_name):
    """
    Return a function that will find a feature in a collection containing the
    point in a given row, and return the value of a property on the feature.
    """
    _finder = find_feature(latcol, lngcol, collection)
    def _getter(row):
        feature = _finder(row)
        return feature['properties'][property_name]
    return _getter

feature_mapping = {}
def find_feature(latcol, lngcol, collection, idx, ndigits=4):
    """
    Return a function that will find a feature in a collection containing the
    point in a given row.
    """
    from shapely.geometry import Point
    def _finder(row):
        try:
            lat = round(float(row[latcol]), ndigits)
            lng = round(float(row[lngcol]), ndigits)
        except ValueError:
            return None

        key = (lat, lng, collection['version'])
        if key in feature_mapping:
            return feature_mapping[key]

        point = Point(lng, lat)

        # query the index for the point to narrow down the search space
        indexed_items = idx.intersection((lng, lat, lng, lat), objects=True)
        for item in indexed_items:
            # search the matched features for one that contains the point
            feature = item.object
            if feature['shape'].contains(point):
                feature_mapping[key] = feature
                return feature
        feature_mapping[key] = None
    return _finder

def rematch(pattern, field, matchgroup=1):
    def _getmatch(row):
        if isinstance(pattern, str):
            pattern_c = re.compile(pattern)
        match = pattern_c.search(row[field])
        if match:
            return match.group(matchgroup)
    return _getmatch

def fuzzy(csvfile, regionfile):
    region_collection, idx = load_shapes(regionfile)

    # Generalize the locations
    zip_pattern = '.*[^\d](\d+)$'
    table = petl.fromcsv(csvfile)\
        .addfields(
            ('pickup_region', find_feature('Pickup Latitude', 'Pickup Longitude', region_collection, idx)),
            ('dropoff_region', find_feature('Dropoff Latitude', 'Dropoff Longitude', region_collection, idx)))\
        .addfields(
            ('Pickup Zip Code', rematch(zip_pattern, 'Pickup Location')),
            ('Pickup Region Centroid Latitude', lambda row: row.pickup_region['centroid'].y if row.pickup_region else None),
            ('Pickup Region Centroid Longitude', lambda row: row.pickup_region['centroid'].x if row.pickup_region else None),
            ('Pickup Region ID', lambda row: row.pickup_region['properties']['OBJECTID'] if row.pickup_region else None),
            ('Dropoff Zip Code', rematch(zip_pattern, 'Dropoff Location')),
            ('Dropoff Region Centroid Latitude', lambda row: row.dropoff_region['centroid'].y if row.dropoff_region else None),
            ('Dropoff Region Centroid Longitude', lambda row: row.dropoff_region['centroid'].x if row.dropoff_region else None),
            ('Dropoff Region ID', lambda row: row.dropoff_region['properties']['OBJECTID'] if row.dropoff_region else None))\
        .addfield('Region Map Version', region_collection['version'])\
        .cutout('pickup_region', 'dropoff_region',
                'Pickup Latitude', 'Pickup Longitude',
                'Dropoff Latitude', 'Dropoff Longitude')

    return table


def upload(csvfile, db_conn_string, table_name, csv_fields, db_fields,
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
        t = petl.fromcsv(csvfile)\
            .cut(csv_fields)\
            .setheader(db_fields)
        petl.todb_upsert(wrap_table(t), 'taxi_trips', db, group_size=group_size)

    return t


@contextmanager
def db_conn(db_conn_string):
    db = datum.connect(db_conn_string)
    yield db
    db.save()
    db.close()


def update_anon(db_conn_str, table_name, column_table_pairs):
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
            logger.info('Updating anonymization table {}'.format(ids))
            sql = make_sql(col, ids)
            db.execute(sql)

def anonymize(csvfile, db_conn_str, field_tuples):
    # download anonymization tables from the db
    anon_mapping = {}
    with db_conn(db_conn_str) as db:
        for csvfield, table, dbfield in field_tuples:
            logger.info('Loading anonymization mapping for {} from {}'.format(csvfield, table))
            queryresults = db.execute('SELECT id, {} FROM {}'.format(dbfield, table))
            anon_mapping[table] = {result[1]: result[0] for result in queryresults}

    # add an anonymized field for each of the fields
    table = petl.fromcsv(csvfile)
    for csvfield, tablename, dbfield in field_tuples:
        logger.info('Setting up anonymization for {}'.format(csvfield))
        table = table.addfield('Anonymized ' + csvfield, lambda row, t=tablename, f=csvfield: anon_mapping[t][row[f]] if row[f] else None)

    return table