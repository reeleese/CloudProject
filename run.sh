#!/bin/bash

# Remove output directory
/usr/local/hadoop-1.2.1/bin/hadoop fs -rmr /project-output/

# Run the spark job
/usr/local/spark-1.6.1-bin-hadoop1/bin/spark-submit --master spark://10.230.119.217:7077 ~/project/gather_data.py

# Copy output to project directory
rm /home/cc/project/processed_data.csv
/usr/local/hadoop-1.2.1/bin/hadoop fs -get /project-output/part-00000 /home/cc/project/processed_data.csv