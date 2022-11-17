"""
Authors: Scott Walters and Marshall Jones
spark_pi_emr.py
"""
import random
import math
from pyspark import SparkContext
import sys
import boto3

def inCircle(point):
    x = 0
    y = 1
    if math.sqrt(point[x]**2 + point[y]**2) < 1:
        return 1
    return 0

def main(argv):
    outBucket = argv[0]
    s3 = boto3.resource('s3')
    obj = s3.Object(outBucket, "output.txt")
    sc = SparkContext(appName = "pi", master = "local")
    
    #Randomly generate many points as (x,y) pairs within the bounds [-1,1]
    nPoints = 100000
    matrix = []
    for count in range(0,nPoints):
       matrix.append([random.uniform(-1,1), random.uniform(-1,1)])
    nums = sc.parallelize(matrix)


    #Count the number of points within the circle
    inside = nums.filter(lambda x: inCircle(x))
    count = inside.count()
    pi = (count / nums.count()) * 4

    text = "Pi estimate: " + str(pi)
    obj.put(Body = text)

if __name__ == "__main__":
    main(sys.argv[1:])

