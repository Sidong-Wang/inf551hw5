from pyspark import SparkContext


import csv
import sys
import re

def map(res):
    line = res.split(',')


    return (line[0], (line[2],1))

def count(a,b):

    return (str(int(a[0])+int(b[0])),a[1]+b[1])



def main(argv):
    sc = SparkContext(appName="hw5")

    rdd = sc.textFile(argv[1])\
            .map(map)\
            .reduceByKey(count)

    output = rdd.collect()



    with open(argv[2], 'w') as out:
        for i in output:

            out.write('%s,\t%s\n' % (i[0], int(i[1][0])/i[1][1]))


if __name__ == '__main__':
    sys.exit(main(sys.argv))