#!/usr/bin/env python

from __future__ import print_function

from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
    def check_valid(row):
        '''Returns true if data is valid, else returns False'''
        valid_binary = lambda x: x in [1, 0]
        valid_age = lambda x: False if x < 18 or x > 60 else True
        valid_corr = lambda x: False if x < -1.0 or x > 1.0 else True
        valid_race = lambda x: x in range(1, 7)

        try:
            if not valid_binary(int(row[0])):
                return False
            if not valid_binary(int(row[1])):
                return False
            if not valid_corr(float(row[2])):
                return False
            if not valid_age(int(row[3])):
                return False
            if not valid_age(int(row[4])):
                return False
            if not valid_race(int(row[5])):
                return False
            if not valid_race(int(row[6])):
                return False
            return True
        except:
            return False

    sc = SparkContext(appName="MySparkProg")
    sc.setLogLevel("ERROR")

    # Get data into RDD, toss header
    data = sc.textFile("hdfs://10.230.119.217:54310/project-input/speed_dating_data.csv")
    header = data.first()
    data = data.filter(lambda x: x != header)

    # Make turn each row of comma seperated values into a list of values
    splitdata = data.mapPartitions(lambda x: reader(x))
    
    # Take only the data we want
    # gender=2, match=12, int_corr=13, age=33, age_o=15, race=39, race_o=16
    splitdata = splitdata.map(lambda x: (x[2], x[12], x[13], x[33], x[15], x[39], x[16]))

    # Clean the data
    splitdata = splitdata.filter(check_valid)
    
    sc.stop()

    # How to get column indices:
    # f = open('speed_dating_data.csv')
    # column_names = f.readline()[:-1].split(',')
    # column_numbers = dict([(column_names[i], i) for i in range(len(column_names))])
