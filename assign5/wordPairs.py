"""
Authors: Marshall Jones and Scott Walters
wordPairs.py
"""
import boto3

def main():
    s3 = boto3.resource("s3")
    files = ["articles_txt.txt"]
    for file in files:
        obj = s3.Object("326-data-bucket", file)

        obj_dict = obj.get()
        obj_stream = obj_dict['Body']
        it = obj_stream.iter_lines(chunk_size=2048)




if __name__ ==  "__main__":
    main()
