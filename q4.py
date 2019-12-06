from pyspark import SparkContext


import csv
import sys
import re

def map(res):
    line = res.split(',')


    return (line[0],line[1])

def count(a,b):

    return a+b



def main(argv):
    sc = SparkContext(appName="hw5")

    freq = sc.textFile(argv[2])
    likes = sc.textFile(argv[1])
    sells = sc.textFile(argv[3])


    rddf = freq.map(map)
    rddl = likes.map(map)
    rddfl = rddf.join(rddl)
    outputs = sells.map(map).collect()





    output = rddfl.collect()




    with open(argv[4], 'w') as out:
        out.write('Drinker\tBeer\n')
        for i in output:
            if(i[1] in outputs)
                out.write(i[0]+'\t'+i[1][1]+'\n')


if __name__ == '__main__':
    sys.exit(main(sys.argv))