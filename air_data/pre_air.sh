#!/bin/bash

PYSPARK_PYTHON=python3.6 spark-submit \
  --master yarn \
  /home/devel/students/2020211019longqianhong/myspark/pre_air.py \
  1000
