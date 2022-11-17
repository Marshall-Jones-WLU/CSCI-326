"""
Authors: Marshall Jones and Scott Walters
spark_numbers.py
"""
from pyspark import SparkContext
import boto3

def main():
    filename = "../datasets/data3/skin_med.txt"
    sc = SparkContext(appName = "assign7", master = "local")
    
    #ex = [x for x in range(20)]
    #rdd1 = sc.parallelize(ex)
    #out = rdd1.collect()
    #print(out)

    rdd = sc.textFile(filename)

    output = rdd.map(lambda line: line.strip().split())

    #print(type(output))
    count = output.count()
    total = output.reduce(lambda a,b: [int(a[0]) + int(b[0]), int(a[1]) + int(b[1]), int(a[2]) + int(b[2])])
    avgs = [total[0] / count, total[1] / count, total[2] / count]
    mini = output.reduce(lambda a,b: [min(int(a[0]), int(b[0])), min(int(a[1]), int(b[1])), min(int(a[2]), int(b[2]))]) 
    maxi = output.reduce(lambda a,b: [max(int(a[0]), int(b[0])), max(int(a[1]), int(b[1])), max(int(a[2]), int(b[2]))]) 


    print("avg: " + str(avgs))
    print("min: " + str(mini))
    print("max: " + str(maxi))



if __name__ == "__main__":
    main()
