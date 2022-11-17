"""
Authors: Marshall Jones and Scott Walters
k_means_spark.py
"""
import sys
import math
import random
import functools
from pyspark import SparkContext

masterDists = []
'''
#randomly sample the next point for a center using weights
def sample(c, points):
    point = random.choices(points[:-1], weights = masterDists, k = 1)[0]
    ndx = points.index(point)
    #print("point chosen: " + str(points[ndx]))
    #print("weight of point chosen: " + str(masterDists[ndx]))
    if points[ndx] in c:
        return sample(c, points)
    else:
        return points[ndx]
'''

#find the distance of all points to its closest center
def findDists(c, points):
    dists = []
    for p in range(0, len(points)-1):
        point = points[p]
        dist = findDist(c, point)
        dists.append(dist)
        #if MasterDists is not fully populated, populate it
        if len(masterDists) < len(dists):
            masterDists.append(dist)
        #if newly computed distance is less than distance store in master list, update
        #master list
        elif dist < masterDists[p]:
            masterDists[p] = dist
    return dists

def findDist(c, point, lower = 0, upper = -1):
    dist = 0
    if upper == -1:
        upper = len(c)
    for ndx in range(lower, upper):
        dist += (float(c[ndx]) - float(point[ndx]))**2
    return [point, c, dist]

def getCentroids(c, points, sc, lower = 0, upper = -1):
    centroids = []
    if upper == -1:
        uppper = len(c[0])
    for i in range(0,len(c)):
        centroids.append([])
    dists = []
    for center in c:
        dists = points.map(lambda p: findDist(center, p, lower, upper))
    dists.reduceByKey(lambda a,b: min(a[2], b[2]))
    
    dists.map(lambda d: centroids[c.index(d[1])].append(d[0]))
    
    #check to ensure all points accounted for
    '''tot = 0
    for col in centroids:
        tot += len(col)
        print(tot)

    if tot == len(points) -1:
        print("match")cd jonesmh22/CSCI-326_1/assignFinal/
    '''
    return centroids
    
def getAvg(lyst, length):
    avgr = []
    for ndx in range(0,len(lyst)):
        avgr.append(lyst[ndx]/length)
    return avgr

def main(argv):
    inFilePath = argv[0]
    inFileName = argv[1]
    inFile = inFilePath + inFileName
    k = int(argv[2])

    sc = SparkContext(appName = "kMeans", master = "local")
    rdd = sc.textFile(inFile)

    if inFile[-3:] == "csv":
        points = rdd.map(lambda line: line.split(','))
    elif inFile[-3:] == "txt":
        points = rdd.map(lambda line: line.split())
    c = points.takeSample(False, k)
    cPrev = []
    #choose k initial points using weighted sampling

    if len(c[0]) <= 3:
        while not cPrev == c:
            cPrev = c
            c = getCentroids(c, points, sc)
            for col in range(0, len(centroids)):
                cent = [0] * len(col[0])

                for item in centroids[col]:
                    for x in range(0,len(item)):
                        cent[x] += float(item[x])

                c[col] = map(lambda a: a / len(centroids[col]), cent)


        text = ""
        text += "Centers:\n" + c + "\nObjective: " + "n/a" + "\n"

    elif len(c[0]) > 3:
        prime = points.takeSample(False, k)
        pPrev = []
        while not cPrev == c:
            cPrev = c
            centroids = getCentroids(c, points, sc, 1, 6)
            for col in range(0, len(centroids)):
                cent = [0] * 5

                for item in centroids[col]:
                    for x in range(1,6):
                        cent[x] += float(item[x])

                map(lambda a: c[col].append(a / len(centroids[col])), cent)
        
        while not pPrev == prime:
            pPrev = prime
            centroids = getCentroids(prime, points, sc, 11, 16)
            for col in range(0, len(centroids)):
                cent = [0] * 5

                for item in centroids[col]:
                    for x in range(11,16):
                        cent[x] += float(item[x])

                map(lambda a:prime[col].append( a / len(centroids[col])), cent)

        text = ""
        text += "Centers 1-5:\n" + str(c) + "\nObjective 1-5: " + "n/a" + "\n\nCenters 11-15:\n" + str(prime) + "\nObjective 11-15: " + "n/a" + "\n"

    outfile = open('k_means_spark_' + inFileName[:-4] + '.txt', 'w')
    outfile.write(text)
    outfile.close()

   # also output the Objective: the sum of the total distance squared from each point to the centers
    # this number should be minimized
    # average is also a cool number

if __name__ == '__main__':
    main(sys.argv[1:])

