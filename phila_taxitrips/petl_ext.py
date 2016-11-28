from datetime import datetime
from glob import iglob
from itertools import chain
from petl import *
from petl.compat import text_type
from .itertools_ext import grouper


def fromcsvs(filepatterns, fieldnames=None, encoding=None, errors='strict', **csvargs):
    """
    Create a table from a list of file names. Fieldnames is an iterable which,
    when specified, is pushed on as the header for the table.
    """
    t = None
    for fname in chain.from_iterable(iglob(p) for p in filepatterns):
        t_partial = fromcsv(fname, encoding=encoding, errors=errors, **csvargs)
        if fieldnames is not None:
            t_partial = t_partial.setheader(fieldnames)
        t = t_partial if t is None else t.cat(t_partial)
    return t


def addfields(table, *field_tuples): #field, value=None, index=None, missing=None):
    """
    Add fields with fixed or calculated values. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['M', 12],
        ...           ['F', 34],
        ...           ['-', 56]]
        >>> # using a fixed value
        ... table2 = etl.addfields(table1,
                                   field('baz', 42),
                                   field('luhrmann', lambda rec: rec['bar'] * 2))
        >>> table2
        +-----+-----+-----+----------+
        | foo | bar | baz | luhrmann |
        +=====+=====+=====+==========+
        | 'M' |  12 |  42 |       24 |
        +-----+-----+-----+----------+
        | 'F' |  34 |  42 |       68 |
        +-----+-----+-----+----------+
        | '-' |  56 |  42 |      112 |
        +-----+-----+-----+----------+

    The tuple elements will be passed along to the `petl.addfield` method.

    """

    return AddFieldsView(table, *field_tuples)


Table.addfields = addfields


def field(name, value=None, index=None):
    return FieldDefinition(name, value=value, index=index)


class FieldDefinition:
    def __init__(self, name, value=None, index=None):
        self.name = name
        self.value = value
        self.index = index


class AddFieldsView(Table):

    def __init__(self, source, *fields, missing=None):
        # ensure rows are all the same length
        self.source = stack(source, missing=missing)
        # convert tuples to FieldDefinitions, if necessary
        self.fields = (field
                       if isinstance(field, FieldDefinition)
                       else FieldDefinition(*field)
                       for field in fields)

    def __iter__(self):
        return iteraddfields(self.source, *self.fields)


def iteraddfields(source, *fields):
    it = iter(source)
    hdr = next(it)
    flds = list(map(text_type, hdr))

    # initialize output fields and indices
    outhdr = list(hdr)
    indices = []

    # determine field indices and construct output fields
    for field in fields:
        index = len(outhdr) if field.index is None else field.index
        indices.append(index)
        outhdr.insert(index, field.name)
    yield tuple(outhdr)

    for row in it:
        outrow = list(row)
        for field, index in zip(fields, indices):
            value = field.value
            if callable(value):
                # wrap rows as records if using calculated value
                row = Record(row, flds)
                v = value(row)
                outrow.insert(index, v)
            else:
                outrow.insert(index, value)
        yield tuple(outrow)


def asmoney(value):
    """Represent the given value as currency"""
    return '{:.2f}'.format(round(float(value), 2))

def asisodatetime(value):
    """Convert a date as YYYY-MM-DD HH:MM:SS"""
    try:
        return datetime\
            .strptime(value.strip(), '%m/%d/%Y %H:%M')\
            .strftime('%Y-%m-%d %H:%M:00')
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

def parsedate(field, pattern):
    def _parsedate_from_row(row):
        try:
            return datetime.strptime(row[field], pattern)
        except ValueError:
            return None
    return _parsedate_from_row

def todb_upsert(table, table_name, db, group_size=1000):
    # Create a list of the column names
    columns = table.fieldnames()
    id_columns = {'Medallion', 'Chauffeur_No', 'Meter_On_Datetime', 'Meter_Off_Datetime'}
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