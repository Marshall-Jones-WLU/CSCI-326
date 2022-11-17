"""
Authors: Marshall Jones and Scott Walters
k_center.py
"""

import sys
import math
import random
import functools

masterDists = []


#find the distance of all points to one center
def findDists(c, points):
    dists = []
    x = 0
    y = 1
    #print(c[0])
    #print(points[0][0])
    for p in range(0, len(points)-1):
        point = points[p]
        #print(points[p])
        dist = (math.sqrt((float(c[x]) - float(point[x]))**2 + (float(c[y]) - float(point[y]))**2))
        dists.append(dist)
        
        #if MasterDists is not fully populated, populate it
        if len(masterDists) < len(dists):
            masterDists.append(dist)
        #if newly computed distance is less than distance store in master list, update
        #master list
        elif dist < masterDists[p]:
            masterDists[p] = dist

def main(argv):
    inFile = argv[0]
    k = int(argv[1])

    
    file = open(inFile, 'r')
    points = file.read().split('\n')
    #data = map(lambda x: x.split())
    #flatmap for spark
    for x in range(0, len(points)):
        points[x] = points[x].split()
    #print(points)
    c = []
    c.append(points[random.randint(0, len(points))])
    while len(c) < k:
        dists = findDists(c[-1], points)
        furthest = functools.reduce(lambda a, b: max(a,b), masterDists)
        ndx = masterDists.index(furthest)
        c.append(points[ndx])
    text = ""
    text += "Centers: " + str(c) + "\nMax Distance: " + str(furthest) + "\n"
    outfile = open('k_center_output.txt', 'w')
    outfile.write(text)
    outfile.close()

if __name__ == "__main__":
    main(sys.argv[1:])
