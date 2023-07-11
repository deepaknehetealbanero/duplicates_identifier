from pyspark.sql.functions import *
from pyspark.sql.functions import lit, col, when
from pyspark.sql import SparkSession
from difflib import SequenceMatcher
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions as F
import time

# --- Global Parms ---
in_parms = dict(in_path = '/home/albanero/PycharmProjects/duplicates_identifier/data/',
                in_file_1 = 'in_data2.csv')
out_parms = dict(out_path = '/home/albanero/PycharmProjects/duplicates_identifier/data_out/')

#Define probable duplicate ratio to get data accordingly.
ratio_perc = 0.4

row_num_grp_id = {}
cnt = 1


# --- udf to get match ratio ---
ratio_udf = udf(lambda ratio: SequenceMatcher(None, ratio[0], ratio[1]).ratio())


def get_list_of_dup_col():
    dup_cols = ()
    conf_file = open('../config/duplicate_identifier_columns.conf', "r")
    for line in conf_file:
        dup_cols = line.split(",")
        print(dup_cols)
    return dup_cols
def read_data(in_parms):
    spark = SparkSession.builder.appName('Duplicates_identifier').getOrCreate()
    in_data = spark.read.csv(in_parms['in_path'] + in_parms['in_file_1'], header='true')
    return in_data, spark
def main():
    global cnt
    dup_cols = get_list_of_dup_col()
    in_data, spark = read_data(in_parms)
    in_data = in_data.withColumn('compare_cols', concat(*dup_cols)).withColumn('row_num', monotonically_increasing_id())
    in_data_2 = in_data.withColumnRenamed('compare_cols', 'compare_cols_1')

    out_data = in_data.crossJoin(in_data_2.select('compare_cols_1'))
    out_data = out_data.withColumn('ratio', ratio_udf(array('compare_cols', 'compare_cols_1')))
    out_data = out_data.filter(col('compare_cols') != col('compare_cols_1')).filter(col('compare_cols') < col('compare_cols_1'))
    in_data.createOrReplaceTempView("in_data_table")
    out_data.createOrReplaceTempView("out_data_table")
    imt_data = spark.sql("select a.row_num, b.row_num as row_num_1, a.ratio "
                          "from out_data_table a join in_data_table b on a.compare_cols_1 == b.compare_cols order by a.ratio desc")

    # grouping logic
    for val in imt_data.rdd.collect():
        rn = str(val.row_num)
        rn1 = str(val.row_num_1)
        ratio = float(val.ratio)

        if rn not in row_num_grp_id and ratio >= ratio_perc:
            row_num_grp_id[rn] = cnt
        elif rn not in row_num_grp_id and ratio < ratio_perc:
            row_num_grp_id[rn] = cnt

        if rn1 not in row_num_grp_id and ratio >= ratio_perc:
            row_num_grp_id[rn1] = row_num_grp_id[rn]

        if rn1 not in row_num_grp_id and ratio < ratio_perc:
            row_num_grp_id[rn1] = cnt + 1
            cnt += 2
        else:
            cnt += 1

    imt_dict = list(map(list, row_num_grp_id.items()))
    grp_id_df = spark.createDataFrame(imt_dict, ["rn", "grp_id"])
    final_data = in_data.join(grp_id_df, in_data.row_num == grp_id_df.rn).drop('rn', 'row_num')

    # print/write data
    final_data.show(200, False)
    final_data.write.csv(out_parms['out_path'], header='true')

if __name__ == '__main__':
    try:
        start = time.time()
        main()
        end = time.time()
        print("Total time taken by the process to execute csv file: {}".format(end-start))
    except Exception as e:
        print("Read Data Failed with error:" + str(e))

