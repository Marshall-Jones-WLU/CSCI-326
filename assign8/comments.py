"""
Authors: Marshall Jones and Scott Walters
comments.py
"""

import sys
import boto3

def main():
    data = ss.read.json("sample.json")
    data_rdd = data.rdd
    print(data_rdd.first())


if __name__ == "__main__":
    main()
