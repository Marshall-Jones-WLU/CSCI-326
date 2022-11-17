'''
Author: Scott Walters and Marshall Jones
bigNeighborhood.py
'''
import math
import functools

#get the indexes of the ten largest neighborhoods
def getTen(nums): 
    ten = {}
    for x in range(0,len(nums)):
        if len(ten) < 10:
            ten[x] = nums[x]
        else:
            check = x
            for value in ten.keys():
                if nums[value] < nums[check]:
                    check = value
            if check in ten.keys():
                ten.pop(check)
                ten[x] = nums[x]
    return ten

#get the points of the ten largest neighborhoods
def findTen(dict, coords):
    ten = []
    for key in dict.keys():
        ten.append(coords[key])
    return ten

#find the distance between two points, return 1 if dist < 25, else 0
def findDist(check, checked):
    x = 0
    y = 1
    dist = math.sqrt((check[x] - checked[x])**2 + (check[y] - checked[y])**2)
    return 1 if dist < 25 else 0

def calculate(coords):
    inDists = [] # nxn matrix of values, 0 if outside 25, 1 if inside 25

    for x in coords:
        dists = []
        for y in coords:
            if x != y: 
                dists.append(findDist(x, y))
        inDists.append(dists)

    neighborhoods = []
    
    for x in inDists:
        neighborhoods.append(functools.reduce(lambda count, y: count + y, x))
    
    return("Average size: " + str(sum(neighborhoods)/len(neighborhoods))+ "\nMax size: " + str(max(neighborhoods)) + "\nTop ten: " + str(findTen(getTen(neighborhoods), coords)))
    
def main():
    files = ["data1/numbers1.txt", "data1/numbers2.txt"]
    for f in range(0, len(files)):
        file = open(files[f], 'r')
        coords = []
        for line in file:
            tcoords = line.split()
            coords.append([int(tcoords[0]), int(tcoords[1])])
        file.close()
        output = open("summary_" + str(f+1) + ".txt", "w")
        data = calculate(coords)
        output.write(data)

        
    

if __name__ == "__main__":
    main()
