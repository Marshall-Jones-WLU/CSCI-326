"""
Authors: Scott Walters and Marshall Jones
spark_numbers_emr.py
"""
import random
import math
from pyspark import SparkContext
import sys
import boto3

def main(argv):
    outBucket = "s3://jonesmh22-cs326"
    inFile = argv[0]
    outFile = argv[1]
    s3 = boto3.resource('s3')
    obj = s3.Object(outBucket, outFile)
    sc = SparkContext(appName = "nums", master = "local")
    rdd = sc.textFile(inFile)

    output = rdd.map(lambda line: line.strip().split())

    #print(type(output))
    count = output.count()
    total = output.reduce(lambda a,b: [int(a[0]) + int(b[0]), int(a[1]) + int(b[1]), int(a[2]) + int(b[2])])
    avgs = [total[0] / count, total[1] / count, total[2] / count]
    mini = output.reduce(lambda a,b: [min(int(a[0]), int(b[0])), min(int(a[1]), int(b[1])), min(int(a[2]), int(b[2]))])
    maxi = output.reduce(lambda a,b: [max(int(a[0]), int(b[0])), max(int(a[1]), int(b[1])), max(int(a[2]), int(b[2]))])


    #print("avg: " + str(avgs))
    #print("min: " + str(mini))
    #print("max: " + str(maxi))
    
    text = "avg: " + agvs  + "\nmin: " +  mini + "\nmax:" + maxi 

    obj.put(Body = text)

if __name__ == "__main__":
    main(sys.argv[1:])

