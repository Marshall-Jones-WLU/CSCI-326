"""
Authors: Marshall Jones and Scott Walters
spark_wc_emr.py
"""
import random
import math
from pyspark import SparkContext, SparkSession
import sys
import boto3

def rmPunc(word):
    for char in word:
        if not char.isalnum():
            word.replace(char,"")
    return word

def main(argv):
    inFile = argv[0]
    outFile = argv[1]
    outBucket = "s3://jonesmh22-cs326"
    s3 = boto3.resource('s3')
    obj = s3.Object(outBucket, outFile)
    sc = SparkContext(appName = "wc", master = "local")


    words = sc.textFile(inFile)
    #spclChars = ["*", "'", '"', ":", ";", ".", "?", "!", ",", "&", "$", "%", "(", ")", "`"]
    clean = words.flatMap(lambda line: line.lower().strip().split(" "))
    #for char in spclChars:
        #print(char)
        #clean = words.map(lambda word: word.replace(char, ""))
    cleaner = clean.map(lambda word: rmPunc(word))

    matrix = cleaner.map(lambda word: [word, 1])
    count = matrix.reduceByKey(lambda a, b: a+b)
    counts = count.sortBy(lambda x: x[1], ascending=False).collect()
    text = ""
    for x in range(0,200):
        text = text + str(counts[x]) + "\n"

    obj.put(Body = text)

if __name__ == "__main__":
    main(sys.argv[1:])

