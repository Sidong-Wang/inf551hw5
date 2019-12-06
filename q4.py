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
    likes = sc.textFile(argv[1])

    rdd1 = freq.map(map)\
                .reduceByKey(count)
    rdd2 = likes.map(map)\
                .reduceByKey(count)




    output = rdd1.subtractByKey(rdd2).collect()




    with open(argv[4], 'w') as out:
        out.write('Drinker\tBeer\n')
        for i in output:

            out.write(i[0]+'\n')


if __name__ == '__main__':
    sys.exit(main(sys.argv))