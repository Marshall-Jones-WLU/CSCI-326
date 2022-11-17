"""
Authors: Scott Walters and Marshall Jones
stream_median.py
"""
import random
import sys
import statistics
import boto3


def getMedians(arr):
    meds = []
    for col in arr:
        meds.append(statistics.median(col))
    return meds

def reservoir(stream, n):
    count = 0
    samp = []

    for line in stream:
        nums = line.decode(encoding='UTF-8').split(",")
        count +=1
        '''if count > 1000:
            break'''
        for x in range(0, len(nums)):
            if len(samp) < x + 1:
                samp.append([float(nums[x])])
            elif len(samp[x]) < n:
                samp[x].append(float(nums[x]))
            else:
                for ndx in range(0, len(samp[x])):
                    rand = random.random()
                    if rand < 1/count:
                        samp[x][ndx] = float(nums[x])
                        break
    return samp


def main(argv):
    inObj = argv[0]
    outFile = argv[1]
    nSamp = int(argv[2])

    s3 = boto3.resource("s3")
    obj = s3.Object("326-data-bucket", inObj)

    obj_dict = obj.get()
    obj_stream = obj_dict['Body']
    it = obj_stream.iter_lines(chunk_size=2048)

    samp = reservoir(it, nSamp)
    meds = getMedians(samp)
    counter = 1
    '''for i in samp:
        print("Sample #" + str(counter), str(i))
        counter += 1
    print("Medians: " + str(meds))'''
    
    out = open(outFile, "w")
    out.write(str(meds))
    out.close()


if __name__ == "__main__":
    main(sys.argv[1:])
