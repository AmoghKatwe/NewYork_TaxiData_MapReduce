from datetime import datetime
from decimal import Decimal, InvalidOperation

from pyspark import SparkContext, StorageLevel


# Strips hour from the given date time string.
def get_hour(date_time):
    return datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S').hour


# Get day of week from the given date time string.
def get_day(date_time):
    return datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S').weekday()


# Returns cell ID for a given latitude & longitude.
def get_cell_id(lat, lon):
    return str(round(lat, 2)) + "-" + str(round(lon, 2))


# Strips date from the given date time string as Y-m-d.
def get_date(date_time):
    return datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S').date()


# Concatenates Cell ID with Hour
def get_cell_id_with_hour(lat, lon, date_time):
    return get_cell_id(Decimal(lat), Decimal(lon)) + "_" + str(get_hour(date_time))


# Concatenates Cell ID with Hour & Date
def get_cell_id_with_hour_and_date(lat, lon, date_time):
    return get_cell_id(Decimal(lat), Decimal(lon)) + "_" + str(get_hour(date_time)) + ":" + str(get_date(date_time))


# Validating the date time string against a given format.
def validate_dt(date_time):
    try:
        datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
        return True
    except ValueError:
        return False


# Validating the given latitude & longitude
def validate_loc(lat, lon):
    try:
        return Decimal(lat) != 0 and Decimal(lon) != 0
    except InvalidOperation:
        return False


if __name__ == '__main__':
    sc = SparkContext("local", "Line Count")
    sc.setLogLevel("INFO")

    # Creating a map of Cell ID vs Places from the given Location Data.
    grid_rdd = sc.textFile("s3://metcs755/USGPSPOI.csv")
    grid_lines = grid_rdd.map(lambda x: x.split('||'))
    poi_list = grid_lines.filter(
        lambda x: len(x) == 4).map(
        lambda line: (
            get_cell_id(Decimal(line[0]), Decimal(line[1])), [line[2]])).reduceByKey(lambda x, y: x + y).collectAsMap()

    # Creating a RDD with cleansed data. This RDD will split the line into different parts of data.
    data_rdd_temp = sc.textFile("s3://metcs755/taxi-data-sorted-small.csv.bz2")
    data_rdd = data_rdd_temp.map(lambda x: x.split(',')).filter(
        lambda x: len(x) == 17 and validate_loc(x[8], x[9]) and validate_dt(x[2]))
    data_rdd.persist(StorageLevel(True, True, False, False, 1))


    # Creating a map of TaxiID vs Array of DriverID
    # Concatenating the array of DriverIDs for each TaxiID in reduce function
    # Creating a map with Length of Unique DriverIDs Array vs TaxiID
    # Sorting by the length in descending order and taking top 10
    print("Running Task 1")
    taxiIDVsDriverIDsMap = data_rdd.filter(
        lambda x: x[1] is not None and x[1] != "").map(
        lambda line: (line[0], [line[1]])).reduceByKey(lambda x, y: x + y).map(
        lambda (x, y): (len(set(y)), x)).sortByKey().top(10)

    for (driverIDCount, taxiID) in taxiIDVsDriverIDsMap:
        print(taxiID + ", " + str(driverIDCount) + "\n")

    
    # Filtering data between 8 AM and 11 AM and then taking out Sunday & Non-Sunday data separately
    # Creating a map of Cell ID vs 1
    # Summing up all the values for each Cell ID in reduce function
    # Interchanging the key and value of previous map in this map function.
    # Sorting by the length in descending order and taking top 20
    print("Running Task 2")
    sunday_group = data_rdd.filter(
        lambda x: 8 <= get_hour(x[3]) <= 11 and get_day(x[3]) == 6).map(
        lambda line: (get_cell_id(Decimal(line[9]), Decimal(line[8])), 1)).reduceByKey(
        lambda x, y: x + y).map(
        lambda (x, y): (y, x)).sortByKey().top(20)

    print("Sunday Group")
    for (no_of_drop_offs, cell_id) in sunday_group:
        if cell_id in poi_list:
            print(cell_id + ", " + str(no_of_drop_offs) + ", " + ",".join(poi_list.get(cell_id)).encode('utf-8') + "\n")
        else:
            print(cell_id + ", " + str(no_of_drop_offs) + "\n")

    non_sunday_group = data_rdd.filter(
        lambda x: 8 <= get_hour(x[3]) <= 11 and get_day(x[3]) != 6).map(
        lambda line: (get_cell_id(Decimal(line[9]), Decimal(line[8])), 1)).reduceByKey(
        lambda x, y: x + y).map(
        lambda (x, y): (y, x)).sortByKey().top(20)

    print("Non-Sunday Group")
    for (no_of_drop_offs, cell_id) in non_sunday_group:
        if cell_id in poi_list:
            print(cell_id + ", " + str(no_of_drop_offs) + ", " + ",".join(poi_list.get(cell_id)).encode('utf-8') + "\n")
        else:
            print(cell_id + ", " + str(no_of_drop_offs) + "\n")

    
    # Drop-off Hour Map:
    # Creating a map of Cell ID with Hour vs Array of Date
    # Concatenating the array of Date for each Cell ID in reduce function
    # Creating a map of Cell ID with Hour vs Average Count of Date
    # Result:
    # Creating a map of Cell ID with Hour & Date vs 1
    # Summing up the values for each key in reduce function
    # Divide the sum by the average drop-off for that hour from the previous map function. Create a map with this value as key and array of Cell ID With Hour & Date and Sum of values
    # Sorting by the key in descending order and taking top 20
    print("Running Task 3")
    drop_offs_hour = data_rdd.map(lambda line: (
        get_cell_id_with_hour(line[9], line[8], line[3]),
        [get_date(line[3])])).reduceByKey(
        lambda x, y: x + y).map(
        lambda (x, y): (x, Decimal(len(y)) / Decimal(len(set(y))))).collectAsMap()

    drop_offs_hour_date = data_rdd.map(lambda line: (
        get_cell_id_with_hour_and_date(line[9], line[8], line[3]), 1)).reduceByKey(
        lambda x, y: x + y).map(
        lambda (x, y): (Decimal(y) / drop_offs_hour.get(x.split(":")[0]), [x, y])).sortByKey().top(20)

    for (key, value) in drop_offs_hour_date:
        cell_id = str(value[0]).split("_")[0]
        if cell_id in poi_list:
            print(cell_id + ", " + str(value[0]).split("_")[1].split(":")[0] + ", " + str(value[0]).split(":")[
                1] + ", " + str(round(key, 2)) + ", " + str(value[1]) + ", " + ",".join(poi_list.get(cell_id)).encode(
                'utf-8') + "\n")
        else:
            print(cell_id + ", " + str(value[0]).split("_")[1].split(":")[0] + ", " + str(value[0]).split(":")[
                1] + ", " + str(round(key, 2)) + ", " + str(value[1]) + "\n")

    
    # Find the peak hours by calculating the hour at which the Surcharge is maximum.
    # Filter data with Surcharge greater than 0
    # Create a map of Hour vs Surcharge
    # Sum up the surcharge for each hour in reduce function
    # Interchange key and value in another map
    # Sorting by the sum of surcharge in descending order and take top 5
    print("Running Task 4")
    peak_hours = data_rdd.filter(
        lambda x: Decimal(x[12]) > 0).map(
        lambda line: (get_hour(line[2]), Decimal(line[12]))).reduceByKey(
        lambda x, y: x + y).map(
        lambda (x, y): (y, x)).sortByKey().top(5)

    for (key, value) in peak_hours:
        print(str(value) + ", " + str(round(key, 2)) + "\n")
