"""
Philadelphia Taxi Data 2015

Reads through raw taxi data.  Create dict and counts of unigue field records.
Transform fields. Add derived data. trip xy dist, trip time, parsed time fields. Unique XYs rounded to 4 dec

Author: Tom Swanson
Created: 9/2/2016
Last Updated: 9/4/2016
Version: 1.0
"""

import os
import sys
import time
from csv import reader
from datetime import datetime
from math import radians, sin, atan2, cos, sqrt, pi


class XYCode:
    def __init__(self, row):
        self.xy_id = row[0]
        self.zip = row[1]
        self.block = row[2]
        self.lon_centroid = row[3]
        self.lat_centroid = row[4]
        self.lon_seg = row[5]
        self.lat_seg = row[6]
        self.seg_id = row[7]


def create_xy_lookup(dir):
    f = open(dir + 'data/XY_Zip_Block.csv', 'r')
    lookup = {}
    try:
        rd = reader(f)
        for row in rd:
            r = XYCode(row)
            lookup[r.xy_id] = "%s,%s,%s,%s,%s,%s,%s" % (r.zip, r.block, r.lon_seg, r.lat_seg, r.lon_centroid, r.lat_centroid,r.seg_id)
    except:
        print('Error opening ' + dir + 'XY_Zip_Block.csv')

    return lookup

# LatLon distance - Haversine method
def distance(lon_1, lat_1, lon_2, lat_2):
    if lon_1 == 0 or lon_2 == 0 or lat_1 == 0 or lat_2 == 0:
        return -1

    # radius = 6371 # km
    radius = 3959  # miles
    dlat = radians(lat_2 - lat_1)
    dlon = radians(lon_2 - lon_1)
    a = sin(dlat / 2) * sin(dlat / 2) + cos(radians(lat_1)) * cos(radians(lat2)) * sin(dlon / 2) * sin(dlon / 2)
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    d = radius * c

    return d


# Line vector compass 360 degree angle
def line_direction_angle(lon_1, lat_1, lon_2, lat_2):
    return atan2(lon_2 - lon_1, lat_2 - lat_1) * 360 / pi


# given "0-360" returns the nearest cardinal direction "N/NE/E/SE/S/SW/W/NW/N"
def get_cardinal(angle):
    directions = 8
    degree = 360 / directions
    angle += degree / 2

    if 0 * degree <= angle < 1 * degree:
        return 'N'
    if 1 * degree <= angle < 2 * degree:
        return 'NE'
    if 2 * degree <= angle < 3 * degree:
        return 'E'
    if 3 * degree <= angle < 4 * degree:
        return 'SE'
    if 4 * degree <= angle < 5 * degree:
        return 'S'
    if 5 * degree <= angle < 6 * degree:
        return 'SW'
    if 6 * degree <= angle < 7 * degree:
        return 'W'
    if 7 * degree <= angle < 8 * degree:
        return 'NW'
    return 'N'

# input data
class TaxiData:
    def __init__(self, row):
        self.trip_id = row[0]
        self.operator_name = row[1]
        self.medallion = row[2]
        self.chauffeur_id = row[3]
        self.pickup_datetime = row[4]
        self.dropoff_datetime = row[5]
        self.trip_distance = row[6]
        self.pickup_latitude = row[7]
        self.pickup_longitude = row[8]
        self.pickup_loc = row[9]
        self.dropoff_latitude = row[10]
        self.dropoff_longitude = row[11]
        self.dropoff_loc = row[12]
        self.fare_amount = row[13].replace('$', '')
        self.tax = row[14].replace('$', '')
        self.tip_amount = row[15].replace('$', '')
        self.surcharge_amount = row[16].replace('$', '')
        self.tolls_amount = row[17].replace('$', '')
        self.total_amount = row[18].replace('$', '')
        self.payment_type = row[19]
        # There is both CC Card and Credit Card, standardize on CC
        if self.payment_type == 'CC CARD' or self.payment_type == 'Credit Card':
            self.payment_type = 'CC'

        self.street_dispatch = row[20]
        # save a few chars.  Only SH and blank for this field
        if self.street_dispatch == 'Street Hail':
            self.street_dispatch = 'SH'
        self.data_source = row[21]
        # only verifone and cmt for this field
        if self.data_source == 'verifone':
            self.data_source = 'vf'

# Here we go
start = time.time()
dir = os.path.dirname(os.path.realpath(__file__)) + "\\"
j = 0
f_out = open(dir + 'taxiout.csv', 'w')
header = 'rec_id,operator_name,medallion,chauffeur_id,pickup_datetime,dropoff_datetime,trip_distance,' \
         'xy_dist,heading,trip_minutes,pickup_latitude,pickup_longitude,pickup_latitude_seg,pickup_longitude_seg,pickup_latitude_centroid,pickup_longitude_centroid,pickup_seg_id,pickup_zip,pickup_block,pickup_month,pickup_day,pickup_hr,pickup_dow,' \
         'dropoff_latitude,dropoff_longitude,dropoff_latitude_seg,dropoff_longitude_seg,dropoff_latitude_centroid,dropoff_longitude_centroid,dropoff_seg_id,dropoff_zip,dropoff_block,dropoff_month,dropoff_day,dropoff_hr,dropoff_dow,fare_amount,tax_amount,' \
         'tip_amount,surcharge_amount,tolls_amount,total_amount,payment_type,street_dispatch,data_source\n'
# Don't write the header out for the table that is will be bulk loaded.  Just for testing.
# f_out.write(header)

xylookup = create_xy_lookup(dir)

begin = time.time()

try:

    f_data = open(dir + '/data/merged_trips.csv', 'rb')

    i = 0

    for line in f_data:
        tempcsv = line.decode("utf-8")
        for temp in reader([tempcsv]):
            ''  # print(temp)

        if len(temp) != 22:
            print('Bad csv read')
            print(temp)
            continue
        if i == 0:
            i += 1
            continue

        data = TaxiData(temp)

        try:
            lon1 = float(data.pickup_longitude)
            lon2 = float(data.dropoff_longitude)
            lat1 = float(data.pickup_latitude)
            lat2 = float(data.dropoff_latitude)
        except:
            lon1 = -75.3
            lon2 = 39.85
            lat1 = -75.3
            lat2 = 39.85
            data.pickup_longitude = '-2'
            data.dropoff_longitude = '-2'
            data.pickup_latitude = '-2'
            data.dropoff_latitude = '-2'

        if data.pickup_longitude != '-2' and lon1 < -76.0 or lon1 > -74.5 or lon2 < -76.0 or lon2 > -74.5 or \
                        lat1 < 39.5 or lat1 > 40.5 or lat2 < 39.5 or lat2 > 40.5:
            lon1 = -75.3
            lon2 = 39.85
            lat1 = -75.3
            lat2 = 39.85
            data.pickup_longitude = '-1'
            data.dropoff_longitude = '-1'
            data.pickup_latitude = '-1'
            data.dropoff_latitude = '-1'

        if len(data.pickup_longitude) > 8:
            data.pickup_longitude = data.pickup_longitude[:8]
        if len(data.dropoff_longitude) > 8:
            data.dropoff_longitude = data.dropoff_longitude[:8]

        if len(data.pickup_latitude) > 7:
            data.pickup_latitude = data.pickup_latitude[:7]
        if len(data.dropoff_latitude) > 7:
            data.dropoff_latitude = data.dropoff_latitude[:7]

        dist = round(distance(lon1, lat1, lon2, lat2), 2)
        if dist > 0:
            heading = int(line_direction_angle(lon1, lat1, lon2, lat2))
            heading_cardinal = get_cardinal(heading)
        else:
            heading = -1
            heading_cardinal = '0'


        XY_id_d = '%s%s' % (str(round(round(lon2 * -1, 4) * 10000)),
                            str(round(round(lat2, 4) * 10000)))


        XY_id_p = '%s%s' % (str(round(round(lon1 * -1, 4) * 10000)),
                            str(round(round(lat1, 4) * 10000)))
        try:
            dropoffzone = xylookup[XY_id_d]
        except:
            dropoffzone = ''

        try:
            pickupzone = xylookup[XY_id_p]
        except:
            pickupzone = ''

        if dropoffzone != '':
            temp = dropoffzone.split(',')
            zip_d = temp[0]
            block_d = temp[1]
            lon_seg_d = temp[2]
            lat_seg_d = temp[3]
            lon_centroid_d = temp[4]
            lat_centroid_d = temp[5]
            seg_id_d  = temp[6]
        else:
            zip_d = ''
            block_d = ''
            lon_seg_d = '0'
            lat_seg_d = '0'
            lon_centroid_d = '0'
            lat_centroid_d = '0'
            seg_id_d = ''

        if pickupzone != '':
            temp = pickupzone.split(',')
            zip_p = temp[0]
            block_p = temp[1]
            lon_seg_p = temp[2]
            lat_seg_p = temp[3]
            lon_centroid_p = temp[4]
            lat_centroid_p = temp[5]
            seg_id_p = temp[6]
        else:
            zip_p = ''
            block_p = ''
            lon_seg_p = '0'
            lat_seg_p = '0'
            lon_centroid_d = '0'
            lat_centroid_d = '0'
            seg_id_d = ''

        pattern = '%Y-%m-%d %H:%M:%S.%f'
        try:

            dt_dropoff = datetime.strptime(data.dropoff_datetime, pattern)
            dropoff_yr = dt_dropoff.year
            dropoff_month = dt_dropoff.month
            dropoff_day = dt_dropoff.day
            dropoff_hr = dt_dropoff.hour
            dropoff_dow = dt_dropoff.strftime("%w")
            dropoff_weekday = dt_dropoff.strftime("%A")
        except ValueError:
            # print('Date error')
            # print(temp)
            data.dropoff_datetime = '1970-01-01 00:00:00.000'
            dt_dropoff = datetime.strptime(data.dropoff_datetime, pattern)
            dropoff_yr = dt_dropoff.year
            dropoff_month = dt_dropoff.month
            dropoff_day = dt_dropoff.day
            dropoff_hr = dt_dropoff.hour
            dropoff_dow = dt_dropoff.strftime("%w")
            dropoff_weekday = dt_dropoff.strftime("%A")

        try:
            dt_pickup = datetime.strptime(data.pickup_datetime, pattern)
            pickup_yr = dt_pickup.year
            pickup_month = dt_pickup.month
            pickup_day = dt_pickup.day
            pickup_hr = dt_pickup.hour
            pickup_dow = dt_pickup.strftime("%w")
            pickup_weekday = dt_pickup.strftime("%A")
        except ValueError:
            # print('Date error')
            # print(temp)
            data.pickup_datetime = '1970-01-01 00:00:00.000'
            dt_pickup = datetime.strptime(data.pickup_datetime, pattern)
            pickup_yr = dt_pickup.year
            pickup_month = dt_pickup.month
            pickup_day = dt_pickup.day
            pickup_hr = dt_pickup.hour
            pickup_dow = dt_pickup.strftime("%w")
            pickup_weekday = dt_pickup.strftime("%A")

        epoch_p = int(time.mktime(time.strptime(data.pickup_datetime, pattern)))
        epoch_d = int(time.mktime(time.strptime(data.dropoff_datetime, pattern)))
        trip_min = 0
        if epoch_d != 0 and epoch_p != 0:
            trip_min = (epoch_d - epoch_p) / 60

        ret = '%i,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
        i,
        data.operator_name,
        data.medallion,
        data.chauffeur_id,
        data.pickup_datetime,
        data.dropoff_datetime,
        data.trip_distance,
        str(dist),
        heading_cardinal,
        str(trip_min),
        data.pickup_latitude,
        data.pickup_longitude,
        lat_seg_p,
        lon_seg_p,
        lat_centroid_p,
        lon_centroid_p,
        seg_id_p,
        zip_p,
        block_p,
        str(pickup_month),
        str(pickup_day),
        str(pickup_hr),
        pickup_dow,
        # pickup_weekday,
        data.dropoff_latitude,
        data.dropoff_longitude,
        lat_seg_d,
        lon_seg_d,
        lat_centroid_d,
        lon_centroid_d,
        seg_id_d,
        zip_d,
        block_d,
        str(dropoff_month),
        str(dropoff_day),
        str(dropoff_hr),
        dropoff_dow,
        # dropoff_weekday,
        data.fare_amount,
        data.tax,
        data.tip_amount,
        data.surcharge_amount,
        data.tolls_amount,
        data.total_amount,
        data.payment_type,
        data.street_dispatch,
        data.data_source)

        j += 1
        i += 1
        # not sure if this works but might go faster to write 1,000 lines at a time
        ret_big = str.join('', (ret_big, ret))
        if j % 1000 == 0:
            f_out.writelines(ret_big)
            ret_big = ''

        if j % 100000 == 0:
            print(j)

            print(time.time() - start, "seconds.")

            start = time.time()

    f_out.writelines(ret_big)
    print(str(i) + ' records processed')
    print(time.time() - begin, "seconds. to loop through records.")
    f_data.close()


except:
    print('Unexpected error: ' + sys.exc_info()[0])


