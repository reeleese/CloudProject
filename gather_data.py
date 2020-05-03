#!/usr/bin/env python

from __future__ import print_function

from csv import reader
from pyspark.mllib.clustering import KMeans
from pyspark import SparkContext
import numpy as np
import datetime

if __name__ == "__main__":
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

    # Clean data before conversion??
    to_print = splitdata.take(3)
    for guy in to_print:
        print(guy)

    sc.stop()

    # How to get column indices:
    # f = open('speed_dating_data.csv')
    # column_names = f.readline()[:-1].split(',')
    # column_numbers = dict([(column_names[i], i) for i in range(len(column_names))])
