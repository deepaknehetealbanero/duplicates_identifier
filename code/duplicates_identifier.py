from pyspark.sql.functions import *
from pyspark.sql.functions import lit, col, when
from pyspark.sql import SparkSession

import time

in_parms = dict(in_path = '/home/albanero/PycharmProjects/duplicates_identifier/data/',
                in_file_1 = 'in_data.csv')
out_parms = dict(out_path = '/home/albanero/PycharmProjects/duplicates_identifier/data_out/')

def get_list_of_dup_col():
    dup_cols = {}
    conf_file = open('../config/duplicate_identifier_columns.conf', "r")
    for line in conf_file:
        k, v = line.split("=")
        dup_cols[k] = v
    return dup_cols
def read_data(in_parms):
    spark = SparkSession.builder.appName('Duplicates_identifier').getOrCreate()
    in_data = spark.read.csv(in_parms['in_path'] + in_parms['in_file_1'], header='true')
    return in_data, spark
def main():
    dup_cols = get_list_of_dup_col()
    print (dup_cols)
    in_data, spark = read_data(in_parms)
    in_data.createTempView("employee_data")
    query = "Select phone, email, count(phone) as count_phone from employee_data " + " group by " + dup_cols['duplicate_col1'] + ", "\
            + dup_cols['duplicate_col2'] +  " having count_phone > 1"
    dup_data = spark.sql(query)
    out_data = in_data.join(dup_data, in_data.phone == dup_data.phone).select(in_data.first_name, in_data.last_name, in_data.gender, in_data.date_of_birth, in_data.address, in_data.city, in_data.state, in_data.country, in_data.email, in_data.phone)
    out_data.show(20, False)
    out_data.write.csv(out_parms['out_path'], header='true')

if __name__ == '__main__':
    try:
        start = time.time()
        main()
        end = time.time()
        print("Total time taken by the process to execute 100k csv file: {}".format(end-start))
    except Exception as e:
        print("Read Data Failed with error:" + str(e))

