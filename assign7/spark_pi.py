"""
Authors: Marshall Jones and Scott Walters
spark_pi.py
"""
import random
import math
from pyspark import SparkContext

def inCircle(point):
    x = 0
    y = 1
    if math.sqrt(point[x]**2 + point[y]**2) < 1:
        return 1
    return 0

def main():
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

    print("Pi estimate: " + str(pi))

if __name__ == "__main__":
    main()
