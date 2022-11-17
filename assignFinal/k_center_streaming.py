"""
Authors: Marshall Jones and Scott Walters
k_center_streaming.py
"""

import sys
import math
import random
import functools
import boto3

#find the distance of all points to one center
def findDists(c, points, lower = 0, upper = -1):
    dists = []
    if upper == -1:
        upper = len(c)
    for p in range(0, len(points)):
        dists.append(distance(points[p][lower:upper], c[lower:upper]))
    return functools.reduce(lambda a,b: min(a,b), dists)

#returns the maximium distance between all centers, or two lists
def getD(c, lower = 0, upper = -1):
    dists = []
    if upper == -1:
        upper = len(c)

    for ndx in range(0, len(c)):
        for i in range(0,len(c)):
            dists.append(distance(c[ndx][lower:upper], c[i][lower:upper]))
    far = functools.reduce(lambda a,b: max(a,b), dists)
    return far

#for two individual points
def distance(a, b):
    dist = 0
    for ndx in range(0, len(a)):
        dist += (float(a[ndx]) -float(b[ndx]))**2
    return math.sqrt(dist)

def main(argv):
    inFile = argv[0]
    k = int(argv[1])
    s3 = boto3.resource('s3')
    outObj = s3.Object('jonesmh22-cs326', "k_center_streaming_output.txt")
    inObj = s3.Object("326-data-bucket", inFile)
    inObjDict = inObj.get()
    inObjStream = inObjDict['Body']
    it = inObjStream.iter_lines(chunk_size = 2048)

    #file = open(inFile, 'r')
    #points = file.read().split('\n')
    #data = map(lambda x: x.split())
    #flatmap for spark
    #for x in range(0, len(points)):
        #points[x] = points[x].split()
    #print(points)
    c = []
    prime = []
    
    for line in it:
        ln = line.decode(encoding = 'UTF-8').split(",")
        c.append(ln)
        prime.append(ln)
        if len(c) >=k:
            break

    #find d
    d = getD(c, 1, 6)
    dP = getD(prime, 11, 16)
    
    for line in it:
        point = line.decode(encoding = 'UTF-8').split(',')
        print(len(point))
        dist = findDists(point, c, 1, 6)
        distP = findDists(point, prime, 11, 16)
        if dist > (d * 2):
            c.append(point)
        if distP > (dP *2):
            prime.append(point)
        if len(c) > k:
            temp = []
            temp.append(c[random.randint(0,len(c)-1)])
            for center in c:
                for t in temp:
                    if distance(center[1:6], t[1:6]) > (d * 2):
                        temp.append(center)
            d = d*2
            c = temp.copy()
        if len(prime) > k:
            temp = []
            temp.append(prime[random.randint(0,len(prime)-1)])
            for center in prime:
                for t in temp:
                    if distance(center[11:16], t[11:16]) > (d * 2):
                        temp.append(center)
            d = d*2
            c = temp.copy()
    
    text = ""
    text += "Centers: " + str(c) + "\nMax Distance: " + str(d) + "\n"
    outfile = open('k_center_streaming_output.txt', 'w')
    outfile.write(text)
    outfile.close()

if __name__ == "__main__":
    main(sys.argv[1:])

