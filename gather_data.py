#!/usr/bin/env python

from __future__ import print_function

import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
    def get_columns(*args):
        '''Given a list of column names as args, returns a list of their
        corresponding column number'''
        
        # How to get this dict:
        # f = open('speed_dating_data.csv')
        # column_names = f.readline()[:-1].split(',')
        # column_numbers = dict([(column_names[i], i) for i in range(len(column_names))])
        column_numbers = {'amb5_1': 96, 'fun3_2': 149, 'sinc3_s': 115,
                          'intel7_2': 124, 'art': 55, 'fun1_s': 111, 'sinc3_3': 186,
                          'prob': 105, 'sinc4_2': 135, 'fun4_3': 176, 'fun3_s': 117,
                          'imprelig': 41, 'pf_o_int': 19, 'expnum': 68, 'yoga': 66,
                          'samerace': 14, 'met': 106, 'amb4_3': 177, 'shar7_2': 127,
                          'fun7_2': 125, 'gaming': 57, 'attr2_1': 81, 'attr1_2': 128,
                          'attr3_1': 87, 'int_corr': 13, 'idg': 3, 'attr4_3': 173,
                          'amb3_s': 118, 'fun4_2': 137, 'museums': 54, 'dec_o': 23,
                          'amb': 102, 'shar': 103, 'them_cal': 157, 'amb_o': 28,
                          'amb5_2': 155, 'sinc4_3': 174, 'fun5_1': 95, 'attr7_2': 122,
                          'position': 7, 'amb1_2': 132, 'sinc2_1': 82, 'attr_o': 24,
                          'field': 34, 'sinc4_1': 76, 'sinc1_1': 70, 'like': 104,
                          'fun1_3': 164, 'amb4_2': 138, 'reading': 59, 'tvsports': 51,
                          'attr3_2': 146, 'shar1_s': 113, 'sinc7_2': 123,'movies': 62,
                          'hiking': 56, 'attr5_3': 190, 'numdat_2': 121, 'shar4_3': 178,
                          'num_in_3': 160, 'sports': 50, 'tuition': 38, 'match_es': 107,
                          'sinc1_s': 109, 'fun3_3': 188, 'intel5_2': 153,
                          'intel3_3': 187, 'pf_o_sha': 22, 'concerts': 63, 'fun7_3': 170,
                          'amb2_2': 144, 'intel1_1': 71, 'condtn': 4, 'clubbing': 58,
                          'age': 33, 'field_cd': 35, 'from': 42, 'intel2_2': 142,
                          'shar4_2': 139, 'sinc1_3': 162, 'shar1_2': 133, 'shar1_3': 166,
                          'intel3_s': 116, 'attr1_s': 108, 'theater': 61, 'partner': 10,
                          'fun2_1': 84, 'intel3_2': 148, 'dec': 97, 'shar_o': 29,
                          'shar2_2': 145, 'pf_o_fun': 20, 'race': 39, 'attr4_1': 75,
                          'sinc7_3': 168, 'exercise': 52, 'sinc1_2': 129, 'sinc5_1': 93,
                          'date': 46, 'date_3': 158, 'goal': 45, 'sinc3_1': 88,
                          'sinc5_2': 152, 'fun2_3': 182, 'satis_2': 119, 'amb3_3': 189,
                          'intel_o': 26, 'zipcode': 43, 'career_c': 49, 'attr3_3': 185,
                          'tv': 60, 'shar4_1': 80, 'intel1_s': 110, 'shar2_3': 184,
                          'go_out': 47, 'intel4_1': 77, 'amb4_1': 79, 'length': 120,
                          'shopping': 65, 'sinc_o': 25, 'amb3_2': 150, 'attr3_s': 114,
                          'like_o': 30, 'fun1_2': 131, 'intel': 100, 'intel2_3': 181,
                          'fun1_1': 72, 'numdat_3': 159, 'sinc': 99, 'career': 48,
                          'shar2_1': 86, 'amb3_1': 91, 'wave': 5, 'intel7_3': 169,
                          'attr2_3': 179, 'pid': 11, 'positin1': 8, 'attr': 98,
                          'fun5_3': 193, 'income': 44, 'intel5_3': 192, 'intel4_3': 175,
                          'amb2_1': 85, 'gender': 2, 'order': 9, 'pf_o_sin': 18, 'iid': 0,
                          'attr5_1': 92, 'met_o': 32, 'amb7_2': 126, 'amb1_1': 73,
                          'attr1_1': 69, 'attr7_3': 167, 'pf_o_amb': 21, 'intel2_1': 83,
                          'fun4_1': 78, 'music': 64, 'amb2_3': 183, 'shar1_1': 74,
                          'attr2_2': 140, 'amb5_3': 194, 'prob_o': 31, 'pf_o_att': 17,
                          'fun3_1': 89, 'amb7_3': 171, 'attr4_2': 134, 'exphappy': 67,
                          'sinc2_2': 141, 'intel3_1': 90, 'sinc3_2': 147, 'age_o': 15,
                          'fun5_2': 154, 'intel5_1': 94, 'undergra': 36, 'match': 12,
                          'shar7_3': 172, 'attr1_3': 161, 'you_call': 156, 'attr5_2': 151,
                          'fun_o': 27, 'intel4_2': 136, 'id': 1, 'sinc2_3': 180,
                          'amb1_s': 112, 'round': 6, 'intel1_3': 163, 'intel1_2': 130,
                          'race_o': 16, 'sinc5_3': 191, 'amb1_3': 165, 'fun': 101,
                          'fun2_2': 143, 'mn_sat': 37, 'imprace': 40, 'dining': 53}

        # Get the columns
        columns = list()
        for column_name in args:
            if column_name not in column_numbers.keys():
                print('Column: {0} not a valid column.').format(column_name)
            else:
                columns.append(column_numbers[column_name])
        return columns
    
    def check_valid(row):
        '''Returns true if data is valid, else returns False'''
        valid_binary = lambda x: x in [1, 0]
        valid_age = lambda x: False if x < 18 or x > 60 else True
        valid_corr = lambda x: False if x < -1.0 or x > 1.0 else True
        valid_race = lambda x: x in range(1, 7)
        valid_one_out_of_ten = lambda x: x in range(1, 11)

        # Skip header
        if row[0] == 'iid':
            return True
        # Check each column value
        try:
            test = int(row[0]) # Just make sure iid is an int. try/catch will get this
            if not valid_binary(int(row[1])):
                return False
            if not valid_binary(int(row[2])):
                return False
            if not valid_corr(float(row[3])):
                return False
            if not valid_age(int(row[4])):
                return False
            if not valid_age(int(row[5])):
                return False
            if not valid_race(int(row[6])):
                return False
            if not valid_race(int(row[7])):
                return False
            if not valid_one_out_of_ten(int(row[8])):
                return False
            return True
        except:
            return False

    def print_dirty_data(row):
        if check_valid(row):
            return True
        else:
            print(row)
            return False

    def make_descriptive(row):
        '''Given a tuple of string values, decodes and casts values to proper type'''
        genders = ['female', 'male']
        match = [True, False]
        races = [
            'Black/African American', 
            'European/Caucasian-American', 
            'Latino/Hispanic American', 
            'Asian/Pacific Islander/Asian-American', 
            'Native American', 
            'Other',
        ]
        # Skip header
        if row[0] == 'iid':
            return row
        
        # Make descriptive
        iid = int(row[0])
        gender = genders[int(row[1])]
        matched = match[int(row[2])]
        correlation = float(row[3])
        age = int(row[4])
        age_o = int(row[5])
        race = races[int(row[6])-1]
        race_o = races[int(row[7])-1]
        imprace = int(row[8])
        return (iid, gender, matched, correlation, age, age_o, race, race_o, imprace)

    # Set up spark
    sc = SparkContext(appName="MySparkProg")
    sc.setLogLevel("ERROR")

    # Get data into RDD, toss header
    data = sc.textFile("hdfs://10.230.119.217:54310/project-input/speed_dating_data.csv")
    # header = data.first()
    # data = data.filter(lambda x: x != header)

    # Fix encoding
    data = data.map(lambda x: x.encode('ascii', 'ignore'))

    # Make turn each row of comma seperated values into a list of values
    splitdata = data.mapPartitions(lambda x: reader(x))
    
    # Take only the data we want
    # gender=2, match=12, int_corr=13, age=33, age_o=15, race=39, race_o=16
    columns_to_take = get_columns('iid', 'gender', 'match', 'int_corr', 'age',
                                  'age_o', 'race', 'race_o', 'imprace')
    splitdata = splitdata.map(lambda x: tuple(x[y] for y in columns_to_take))

    # Clean the data
    print('Size of dataset before cleaning: {0}'.format(splitdata.count()))
    # For getting the dirty rows
    # for row in splitdata.take(5):
    #     check_valid_wrapper(row)
    # sc.stop()
    
    splitdata = splitdata.filter(check_valid)
    print('Size of dataset after cleaning: {0}'.format(splitdata.count()))

    # Map data to correct types
    descriptive_data = splitdata.map(make_descriptive)

    # Save result as csv
    csv_ify = lambda row: ','.join(str(val) for val in row)
    lines = descriptive_data.map(csv_ify)
    lines.coalesce(1, shuffle=True).saveAsTextFile('hdfs://10.230.119.217:54310/project-output/')
    
    sc.stop()


