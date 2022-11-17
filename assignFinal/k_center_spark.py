"""
Authors: Marshall Jones and Scott Walters
k_center_spark.py
"""

import sys
import math
import random
import functools
from pyspark import SparkContext


def distHelper(centers, point, lower, upper):
    dist = 0
    minDist = None
    for c in range(0, len(centers)):
        for p in range(lower, upper):
            #print(point[p])
            #print(centers[c][p])
            dist += (float(point[p]) - float(centers[c][p]))**2
        dist = math.sqrt(dist)
        if minDist == None or dist < minDist:
            minDist = dist
    return [minDist, point]

def findDists(c, points, lower = 0, upper = -1):
    if upper == -1:
        upper = len(c[0])
    dists = points.map(lambda point: distHelper(c, point, lower, upper), points)
    return dists
    
def main(argv):
    inFilePath = argv[0]
    inFileName = argv[1]
    inFile = inFilePath + inFileName
    k = int(argv[2])

    sc = SparkContext(appName = "kCenter", master = "local")
    rdd = sc.textFile(inFile)

    #separate into individual coordinates
    if inFile[-3:] == "csv":
        points = rdd.map(lambda line: line.split(','))
    elif inFile[-3:] == "txt":
        points = rdd.map(lambda line: line.split())
    c = points.takeSample(False, 1)
    if len(c[0]) <=3:
        while len(c) < k:
            dists = findDists(c, points)
            furthest = dists.takeOrdered(1, key = lambda x: -x[0])
            c.append(furthest[0][1])
        dists = findDists(c,points)
        furthest = dists.takeOrdered(1, key = lambda x: -x[0])
        text = ""
        text += "Centers: " + str(c) + "\nMax Distance: " + str(furthest[0][0]) + "\n"

    elif len(c[0]) > 3:
        prime = points.takeSample(False, 1)
        while len(c) < k and len(prime) < k:
            distsC = findDists(c, points, 1, 6)
            distsP = findDists(c, points, 11, 17)
            farC = distsC.takeOrdered(1, key = lambda x: -x[0])
            farP = distsP.takeOrdered(1, key = lambda x: -x[0])
            c.append(farC[0][1])
            prime.append(farP[0][1])
        distsC = findDists(c, points, 1, 6)
        distsP = findDists(c, points, 11, 17)
        farC = distsC.takeOrdered(1, key = lambda x: -x[0])
        farP = distsP.takeOrdered(1, key = lambda x: -x[0])
        text = ""
        text += "Centers 1-5: " + str(c) + "\nMax Distance 1-5: " + str(farC[0][0]) + "\n\nCenters 11-15: " + str(prime) + "\nMax Distance 11-15: " + str(farP[0][0]) + "\n"

    outfile = open('k_center_spark_' + inFileName[:-4] + '.txt', 'w')
    outfile.write(text)
    outfile.close()

if __name__ == "__main__":
    main(sys.argv[1:])
