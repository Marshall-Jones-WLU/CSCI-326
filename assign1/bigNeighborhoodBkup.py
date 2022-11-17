'''
Author: Scott Walters and Marshall Jones
bigNeighborhood.py
'''
import math
import functools

def findDist(check, checked):
    x = 0
    y = 1
    dist = math.sqrt((check[x] - checked[x])**2 + (checked[y] - checked[y])**2)
    return 1 if dist < 25 else 0

def getStats(neighborhoods):
    # get the average neighborhood size
    avg = sum(neighborhoods)/len(neighborhoods)

    # get the biggest neighborhood
    neighborhoods.sort()
    biggest = neighborhoods[-1]

    # get the 10 biggest neighborhoods
    top10 = neighborhoods[-10:]

    print("Average size: " + str(avg))
    print("Max size: " + str(biggest))
    print("10 Biggest: " + str(top10))

def main():
    files = ["data1/numbers1.txt", "data1/numbers2.txt"]
    for f in files:
        txt = open(f, 'r')
        coords = []
        for line in txt:
            tcoords = line.split()
            coords.append([int(tcoords[0]), int(tcoords[1])])
        txt.close()

        inDists = [] # 0 if outside 25, 1 if inside 25

        for x in coords:
            dists = []
            for y in coords:
                if x != y: 
                    dists.append(findDist(x, y))
            inDists.append(dists)
        
        neighborhoods = []
        
        for x in inDists:
            neighborhoods.append(functools.reduce(lambda count, y: count + y, x))

        #print(neighborhoods)
        
        output = open(str(numbers[f].out), "w")
        output.write(getStats(neighborhoods))

if __name__ == "__main__":
    main()
