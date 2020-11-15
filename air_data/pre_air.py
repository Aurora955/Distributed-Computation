#配置环境
import findspark
findspark.init('/usr/lib/spark-current')
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Python Spark with DataFrame").getOrCreate()

#读入数据
from pyspark.sql.types import *
schema_sdf = StructType([
        StructField('Year', IntegerType(), True),
        StructField('Month', IntegerType(), True),
        StructField('DayofMonth', IntegerType(), True),
        StructField('DayOfWeek', IntegerType(), True),
        StructField('DepTime', DoubleType(), True),
        StructField('CRSDepTime', DoubleType(), True),
        StructField('ArrTime', DoubleType(), True),
        StructField('CRSArrTime', DoubleType(), True),
        StructField('UniqueCarrier', StringType(), True),
        StructField('FlightNum', StringType(), True),
        StructField('TailNum', StringType(), True),
        StructField('ActualElapsedTime', DoubleType(), True),
        StructField('CRSElapsedTime',  DoubleType(), True),
        StructField('AirTime',  DoubleType(), True),
        StructField('ArrDelay',  DoubleType(), True),
        StructField('DepDelay',  DoubleType(), True),
        StructField('Origin', StringType(), True),
        StructField('Dest',  StringType(), True),
        StructField('Distance',  DoubleType(), True),
        StructField('TaxiIn',  DoubleType(), True),
        StructField('TaxiOut',  DoubleType(), True),
        StructField('Cancelled',  IntegerType(), True),
        StructField('CancellationCode',  StringType(), True),
        StructField('Diverted',  IntegerType(), True),
        StructField('CarrierDelay', DoubleType(), True),
        StructField('WeatherDelay',  DoubleType(), True),
        StructField('NASDelay',  DoubleType(), True),
        StructField('SecurityDelay',  DoubleType(), True),
        StructField('LateAircraftDelay',  DoubleType(), True)
    ])
air = spark.read.options(header='true').schema(schema_sdf).csv("/lqhspark/air8000.csv")
used_var=["Year","Month","DayofMonth","DayOfWeek","DepTime","CRSDepTime","CRSArrTime","UniqueCarrier","ActualElapsedTime","Origin","Dest","ArrDelay","AirTime","Distance"]
air2_pdf = air.select(used_var).toPandas()
myair=air2_pdf.dropna()
myair.index=range(len(myair))#一定要更新索引！!!否则会出现空值
print("myair:",myair)
#选择哑变量
#! /usr/bin/env python3
import pandas as pd
import numpy as np
from collections import Counter

def dummy_factors_counts(pdf, dummy_columns):
    '''Function to count unique dummy factors for given dummy columns
    pdf: pandas data frame
    dummy_columns: list. Numeric or strings are both accepted.
    return: dict same as dummy columns
    '''
    # Check if current argument is numeric or string
    pdf_columns = pdf.columns.tolist()  # Fetch data frame header

    dummy_columns_isint = all(isinstance(item, int) for item in dummy_columns)
    if dummy_columns_isint:
        dummy_columns_names = [pdf_columns[i] for i in dummy_columns]
    else:
        dummy_columns_names = dummy_columns

    factor_counts = {}
    for i in dummy_columns_names:
        factor_counts[i] = (pdf[i]).value_counts().to_dict()

    return factor_counts


def select_dummy_factors(dummy_dict, keep_top, replace_with):
    '''Merge dummy key with frequency in the given file
    dummy_dict: dummy information in a dictionary format
    keep_top: list
    '''
    dummy_columns_name = list(dummy_dict)
    nobs = sum(dummy_dict[dummy_columns_name[1]].values())

    factor_set = {}  # The full dummy sets
    factor_selected = {}  # Used dummy sets
    factor_dropped = {}  # Dropped dummy sets
    factor_selected_names = {}  # Final revised factors

    for i in range(len(dummy_columns_name)):

        column_i = dummy_columns_name[i]

        factor_set[column_i] = list((dummy_dict[column_i]).keys())

        factor_counts = list((dummy_dict[column_i]).values())
        factor_cumsum = np.cumsum(factor_counts)
        factor_cumpercent = factor_cumsum / factor_cumsum[-1]

        factor_selected_index = np.where(factor_cumpercent <= keep_top[i])
        factor_dropped_index = np.where(factor_cumpercent > keep_top[i])

        factor_selected[column_i] = list(
            np.array(factor_set[column_i])[factor_selected_index])

        # Replace dropped dummies with indicators like `others`
        if len(factor_dropped_index[0]) == 0:
            factor_new = []
        else:
            factor_new = [replace_with]

        factor_new.extend(factor_selected[column_i])

        factor_selected_names[column_i] = [
            column_i + '_' + str(x) for x in factor_new
        ]

    dummy_list=list(factor_selected_names.values())
    
    return dummy_list

#将字符型数字转化为浮点型
def safe_float(item):
    try:
        n=float(item)
    except:
        n=item
    return n

#求各个哑变量的值
sample_num=len(myair)
def get_dummy_value(dummy_list,replace_with,myair):
    new_air=pd.DataFrame()
    for i in dummy_list:
        if i[0].split("_")[1]==replace_with:
            other=pd.Series([0]*sample_num)
            for j in i[1:]:
                new_air[j]=(myair[j.split("_")[0]])==safe_float(j.split("_")[1])
            new_air=new_air.replace([True,False],[1,0])
            for j in i[1:]:
                other+=new_air[j]
            new_air[i[0]]=pd.Series([1]*sample_num)-other
        else:
            for j in i:
                new_air[j]=(myair[j.split("_")[0]])==safe_float(j.split("_")[1])
    new_air=new_air.replace([True,False],[1,0])
    return new_air

dummy_columns = ['Year', 'Month', 'DayOfWeek', 'UniqueCarrier', 'Origin', 'Dest']#需要选择哑变量的列
dummy_counts=dummy_factors_counts(myair, dummy_columns)#各个哑变量的频数
keep_top = [1,1,1, 0.8, 0.8, 0.8]#最大累计百分比
replace_with="others"
dummy_list=select_dummy_factors(dummy_counts, keep_top, replace_with)
#new_air.loc[new_air["Dest_DAL"]!=0,"Dest_DAL"]
new_air=get_dummy_value(dummy_list,replace_with,myair)
other_var=list(set(used_var)^set(dummy_columns))
new_air=pd.concat([myair[other_var],new_air],axis=1)
# for col in new_air.columns:
#     if new_air[col].isnull().sum()>0:
#         del new_air[col]

#new_air["Year_2004.0"].isnull().sum()

new_air['ArrDelay']=new_air['ArrDelay']>0
new_air['ArrDelay']=new_air['ArrDelay'].replace([True,False],[1,0])
#print("sample_num is",sample_num)
print("new_air:",new_air)
print("dummy_count:",dummy_counts)
print("dummy_list:",dummy_list)
myair.to_csv("/home/devel/students/2020211019longqianhong/myspark/myair.csv",index=False)
new_air.to_csv("/home/devel/students/2020211019longqianhong/myspark/new_air.csv",index=False)