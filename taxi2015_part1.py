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


# print unigue field counts dictionaries
def print_unique_count_list(dict_c, file_name):
    f = open(dir + file_name, 'w')
    for k_item, k in dict_c.items():
        tmp = '%s,%s\n' % (k, k_item)
        f.writelines(tmp)
    f.close()


# update field unique count dict
def update_count_dict(dict_c, d):
    if not d in dict_c:
        dict_c[d] = 1
    else:
        dict_c[d] += 1


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


# Globals
badCoord = 0
medallion_c = {}
trip_id_c = {}
operator_name_c = {}
chauffeur_id_c = {}
trip_distance_c = {}
pickup_loc_c = {}
dropoff_loc_c = {}
fare_amount_c = {}
tax_c = {}
tip_amount_c = {}
surcharge_amount_c = {}
tolls_amount_c = {}
total_amount_c = {}
payment_type_c = {}
street_dispatch_c = {}
data_source_c = {}
XYp = ''
XY_cp = {}
XYd = ''
XY_cd = {}
XY = ''
XY_c = {}
ret_big = ''

# Here we go
start = time.time()
dir = os.path.dirname(os.path.realpath(__file__)) + "\\"
j = 0

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

        # Just for testing
        update_count_dict(medallion_c, data.medallion)
        update_count_dict(trip_id_c, data.trip_id)
        update_count_dict(operator_name_c, data.operator_name)
        update_count_dict(chauffeur_id_c, data.chauffeur_id)
        update_count_dict(trip_distance_c, data.trip_distance)
        update_count_dict(pickup_loc_c, data.pickup_loc)
        update_count_dict(dropoff_loc_c, data.dropoff_loc)
        update_count_dict(fare_amount_c, data.fare_amount)
        update_count_dict(tax_c, data.tax)
        update_count_dict(tip_amount_c, data.tip_amount)
        update_count_dict(surcharge_amount_c, data.surcharge_amount)
        update_count_dict(tolls_amount_c, data.tolls_amount)
        update_count_dict(total_amount_c, data.total_amount)
        update_count_dict(payment_type_c, data.payment_type)
        update_count_dict(street_dispatch_c, data.street_dispatch)
        update_count_dict(data_source_c, data.data_source)

        try:
            lon1 = float(data.pickup_longitude)
            lon2 = float(data.dropoff_longitude)
            lat1 = float(data.pickup_latitude)
            lat2 = float(data.dropoff_latitude)
        except:
            lon1 = 0
            lon2 = 0
            lat1 = 0
            lat2 = 0
            data.pickup_longitude = '-2'
            data.dropoff_longitude = '-2'
            data.pickup_latitude = '-2'
            data.dropoff_latitude = '-2'

        if data.pickup_longitude != '-2' and lon1 < -76.0 or lon1 > -74.5 or lon2 < -76.0 or lon2 > -74.5 or \
                        lat1 < 39.5 or lat1 > 40.5 or lat2 < 39.5 or lat2 > 40.5:
            lon1 = 0
            lon2 = 0
            lat1 = 0
            lat2 = 0
            data.pickup_longitude = '-1'
            data.dropoff_longitude = '-1'
            data.pickup_latitude = '-1'
            data.dropoff_latitude = '-1'
            badCoord += 1

        if len(data.pickup_longitude) > 8:
            data.pickup_longitude = data.pickup_longitude[:8]
        if len(data.dropoff_longitude) > 8:
            data.dropoff_longitude = data.dropoff_longitude[:8]

        if len(data.pickup_latitude) > 7:
            data.pickup_latitude = data.pickup_latitude[:7]
        if len(data.dropoff_latitude) > 7:
            data.dropoff_latitude = data.dropoff_latitude[:7]

        # dropoff XY
        XY = '%s%s,%s,%s' % (str(round(round(lon2 * -1, 4) * 10000)),
                             str(round(round(lat2, 4) * 10000)),
                             str(round(lon2, 4)),
                             str(round(lat2, 4)))

        # This is for creating the unique list of Lat/Lon
        update_count_dict(XY_c, XY)

        XY_id_d = '%s%s' % (str(round(round(lon2 * -1, 4) * 10000)),
                            str(round(round(lat2, 4) * 10000)))
        # pickup XY
        XY = '%s%s,%s,%s' % (str(round(round(lon1 * -1, 4) * 10000)),
                             str(round(round(lat1, 4) * 10000)),
                             str(round(lon1, 4)),
                             str(round(lat1, 4)))

        # This is for creating the unique list of Lat/Lon
        update_count_dict(XY_c, XY)

        XY_id_p = '%s%s' % (str(round(round(lon1 * -1, 4) * 10000)),
                            str(round(round(lat1, 4) * 10000)))

        j += 1
        i += 1

        if j % 100000 == 0:
            print(j)

            print(time.time() - start, "seconds.")

            start = time.time()

    # f_out.writelines(ret_big)
    print(str(i) + ' records processed')
    print(time.time() - begin, "seconds. to loop through records.")
    f_data.close()


except:
    print('Unexpected error: ' + sys.exc_info()[0])

# Just for testing
print_unique_count_list(trip_id_c, 'trip_id_c.csv')
print_unique_count_list(operator_name_c, 'operator_name_c.csv')
print_unique_count_list(chauffeur_id_c, 'chauffeur_id_c.csv')
print_unique_count_list(medallion_c, 'medallion_c.csv')
print_unique_count_list(trip_distance_c, 'trip_distance_c.csv')
print_unique_count_list(pickup_loc_c, 'pickup_loc_c.csv')
print_unique_count_list(dropoff_loc_c, 'dropoff_loc_c.csv')
print_unique_count_list(fare_amount_c, 'fare_amount_c.csv')
print_unique_count_list(tax_c, 'tax_c.csv')
print_unique_count_list(tip_amount_c, 'tip_amount_c.csv')
print_unique_count_list(surcharge_amount_c, 'surcharge_amount_c.csv')
print_unique_count_list(tolls_amount_c, 'tolls_amount_c.csv')
print_unique_count_list(total_amount_c, 'total_amount_c.csv')
print_unique_count_list(payment_type_c, 'payment_type_c.csv')
print_unique_count_list(street_dispatch_c, 'street_dispatch_c.csv')
print_unique_count_list(data_source_c, 'data_source_c.csv')

# Use this for the unique list of Lat/Lon
print_unique_count_list(XY_c, 'XY_c.csv')

print(time.time() - begin, "seconds total.")
