'''
Author: Scott Walters and Marshall Jones
cover.py
'''
import math
import functools
import bigNeighborhood as bn
import sys

def greedy(coords, dist):
    data = bn.matrix(coords, dist)
    neighborhoods = []
    for x in data:
        neighborhoods.append(functools.reduce(lambda count, y: count + y, x))
    
    covered = []
    points = []
    while len(covered) != len(data):
        biggest = max(neighborhoods)
        cover = data[neighborhoods.index(biggest)]
        covered.append(coords[neighborhoods.index(biggest)])
        points.append(coords[neighborhoods.index(biggest)])
        neighborhoods[neighborhoods.index(biggest)] = -1
        for i in range (0,len(cover)):
            if cover[i] == 1:
                neighborhoods[i] = -1
                covered.append(coords[i])
    return points

def main(argv):
    inFile = argv[0]
    outFile = argv[1]
    if len(argv) > 2:
        dist = int(argv[2])
    else:
        dist = 25
    #files = ["data1/numbers1.txt", "data1/numbers2.txt"]
    files = [inFile]
    for f in range(0, len(files)):
        file = open(files[f], 'r')
        coords = []
        for line in file:
            tcoords = line.split()
            coords.append([int(tcoords[0]), int(tcoords[1])])
        file.close()
        #output = open("cover_" + str(f+1) + ".txt", "w")
        output = open(outFile, "w")
        data = greedy(coords, dist)
        for line in data:
            output.write(str(line) + "\n")
        output.close()


if __name__ == "__main__":
    main(sys.argv[1:])
