"""
Authors: Marshall Jones and Scott Walters
spark_wc.py
"""
from pyspark import SparkContext
import sys

def rmPunc(word):
    for char in word:
        if not char.isalnum():
            word.replace(char,"")
    return word

def main(argv):
    inFile = argv[0]
    outFile = argv[1]
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
    for x in range(0,50):
        print(counts[x])


if __name__ == "__main__":
    main(sys.argv[1:])
