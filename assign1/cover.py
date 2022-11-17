'''
Author: Scott Walters and Marshall Jones
cover.py
'''
import math
import functools
import bigNeighborhood.py as bn

def greedy(coords):
    data = bn.matrix(coords)
    neighborhoods = []
    for x in data:
        neighborhoods.append(functools.reduce(lambda count, y: count + y, x))
    
    covered = []
    while len(covered) != len(data):
        biggest = max(neighborhoods)
        cover = data[neighborhoods.index(biggest)]
        covered.append(cover)
        for i in neighborhoods:
            if bn.findDist(data[i]) == 1:
                neighborhoods[i] = -1
    return covered

def main():
    #files = ["data1/numbers1.txt", "data1/numbers2.txt"]
    files = ["data1/numbers1.txt"]
    for f in range(0, len(files)):
        file = open(files[f], 'r')
        coords = []
        for line in file:
            tcoords = line.split()
            coords.append([int(tcoords[0]), int(tcoords[1])])
        file.close()
        output = open("cover_" + str(f+1) + ".txt", "w")
        data = greedy(coords)
        output.write(data)
        output.close()

if __name__ == "__main__":
    main()
