from pyspark import SparkContext


import csv
import sys
import re

def map(res):
    line = res.split(',')


    return (line[0],1)

def count(a,b):

    return a+b



def main(argv):
    sc = SparkContext(appName="hw5")

    freq = sc.textFile(argv[1])
    likes = sc.textFile(argv[2])

    rdd1 = freq.map(map)\
                .reduceByKey(count)
    rdd2 = likes.map(map)\
                .reduceByKey(count)


    output1 = rdd1.collect()
    output2 = rdd2.collect()
    rdd3= sc.parallelize([a[0] for a in output1]).distinct()
    rdd4 = sc.parallelize([a[0] for a in output2]).distinct()
    output = rdd3.subtract(rdd4).collect()
    print(output)




    with open(argv[3], 'w') as out:
        out.write('Drinker\n')
        for i in output:

            out.write(i+'\n')


if __name__ == '__main__':
    sys.exit(main(sys.argv))