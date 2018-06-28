from __future__ import print_function
import os

os.environ["SPARK_HOME"] = "C:\\Users\\Walter\\Spark-Hadoop\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"

from pyspark import SparkContext, StorageLevel

def mul(elements):

    m1 = list()
    m2 = list()
    sum = list()
    for _lst in elements[1]:
        if str(_lst[0]) == "A":
            m1.append([str(_lst[1]), str(_lst[2])])
        else:
            m2.append([str(_lst[1]), str(_lst[2])])

    for _elA in m1:
        for _elB in m2:
            sum.append(((str(_elA[0]), str(_elB[0])), int(_elA[1]) * int(_elB[1])))
    return sum

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    M1 = sc.textFile('Matrix1')
    M2 = sc.textFile('Matrix2')

    rdd_M1 = M1.map(lambda x: x.split(',')).map(lambda data: (data[1], ['A', data[0], data[2]]))
    rdd_M2 = M2.map(lambda x: x.split(',')).map(lambda data: (data[0], ['B', data[1], data[2]]))
    rdd_result1 = rdd_M1.union(rdd_M2).groupByKey().map(lambda x: (x[0], list(x[1])))

    rdd_reducer1 = rdd_result1.flatMap(lambda e: mul(e)).persist(StorageLevel.MEMORY_ONLY_SER)

    rdd_M1_M2 = rdd_reducer1.map(lambda x: x)
    rdd_reducer2 = rdd_M1_M2.reduceByKey(lambda x, y: x + y)
    rdd_result2 = rdd_reducer2.collect()
    rdd_result2.sort()

    for _x in rdd_result2:
        print("%s,%s\t%d" % (_x[0][0], _x[0][1], _x[1]))
