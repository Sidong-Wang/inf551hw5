from pyspark import SparkContext


import csv
import sys
import re

def map(res):
    print(res)
    line = res.split(',')
    pattern = 'Bud.*'
    if int(line[2])<=5 and re.match(pattern, line[1]):
        return (line[0], float(1))
    if int(line[2])>5 and re.match(pattern, line[1]):
        return (line[0], float(-1))

def count(a,b):
    if a<0 or b<0:
        return -1
    return a+b



def main(argv):
    sc = SparkContext(appName="hw5")

    rdd = sc.textFile(argv[1])\
            .map(map)\
            .reduceByKey(count)

    output = rdd.collect()



    with open(argv[2], 'w') as out:
        for i in output:

            out.write('%s, %s' % (v[0], v[1]))


if __name__ == '__main__':
    sys.exit(main(sys.argv))