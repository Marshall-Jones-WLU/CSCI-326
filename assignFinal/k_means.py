"""
Authors: Marshall Jones and Scott Walters
k_means.py
"""

import sys
import math
import random
import functools

masterDists = []

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

#find the distance of all points to its closest center
def findDists(c, points):
    dists = []
    x = 0
    y = 1
    #print(c[0])
    #print(points[0][0])
    for p in range(0, len(points)-1):
        point = points[p]
        #print(points[p])
        dist = ((float(c[x]) - float(point[x]))**2 + (float(c[y]) - float(point[y]))**2)
        dists.append(dist)

        #if MasterDists is not fully populated, populate it
        if len(masterDists) < len(dists):
            masterDists.append(dist)
        #if newly computed distance is less than distance store in master list, update
        #master list
        elif dist < masterDists[p]:
            masterDists[p] = dist
    return dists

def findDist(c, point):
    x = 0
    y = 1
    dist = ((float(c[x]) - float(point[x]))**2 + (float(c[y]) - float(point[y]))**2)
    return dist

def getCentroids(c, points):
    centroids = []
    for i in range(0,len(c)):
        centroids.append([])
    
    for ndx in range(0, len(points)-1):
        dists = []
        for center in range(0, len(c)):
            dists.append(findDist(c[center], points[ndx]))
        close = functools.reduce(lambda a,b: min(a,b), dists)
        i = dists.index(close)
        #print("index: " +str(i))
        centroids[i].append(points[ndx])
    
    #check to ensure all points accounted for
    '''tot = 0
    for col in centroids:
        tot += len(col)
        print(tot)

    if tot == len(points) -1:
        print("match")
    '''
    return centroids

def main(argv):
    inFile = argv[0]
    k = int(argv[1])

    file = open(inFile, 'r')
    points = file.read().split('\n')

    #flatmap for spark
    for x in range(0, len(points)):
        points[x] = points[x].split()
    c = []
    
    '''
    for center in k:
        cNdx = random.randint(0,len(points))
        c.append(points[cNdx])
    '''

    #choose k initial points using weighted sampling
    c.append(points[random.randint(0,len(points))])
    dists = []
    while len(c) < k:
        dists = findDists(c[-1], points)
        #closest = functools.reduce(lambda a, b: min(a,b), dists)
        
        c.append(sample(c, points))
        #print("centers: " + str(c))
        furthest = functools.reduce(lambda a, b: max(a,b), masterDists)
        #print("furthest: " + str(furthest))
    findDists(c[-1],points)
    #move each point to its centriod

    #create where each column represents the points within an individual center
    #take mean of each column and set that to be the center
    #may be a nested map for spark
    cprev = []
    while not cprev == c:
        cprev = c.copy()
        centroids = getCentroids(c, points)
        for col in range(0, len(centroids)):
            x = 0
            y = 0
            #print(len(centroids[col]))
            for item in centroids[col]:
                x += float(item[0])
                y += float(item[1])
            c[col] = [(x / len(centroids[col])), (y / len(centroids[col]))]
    
        for ndx in range(0,len(c)):
            findDists(c[ndx], points)
    print(c)
    # also output the Objective: the sum of the total distance squared from each point to the centers
    # this number should be minimized
    # average is also a cool number

if __name__ == '__main__':
    main(sys.argv[1:])

