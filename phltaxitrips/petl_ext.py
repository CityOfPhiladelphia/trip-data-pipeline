from datetime import datetime
from glob import iglob
from itertools import chain
from petl import *
from .itertools_ext import grouper


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

def todb_upsert(table, table_name, db, group_size=1000):
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
        db._c.executemany(sql, list_of_rows)
    db.save()

Table.todb_upsert = todb_upsert