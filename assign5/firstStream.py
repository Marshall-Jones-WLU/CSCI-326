"""
Authors: Marshall Jones and Scott Walters
firstStream.py
"""
import boto3

def output(avgs, maxs, mins, nonZ, count):
    out = ""
    out += "count: " + str(count)
    out += "\navgs: " + str(avgs)
    out += "\nmaxs: " + str(maxs)
    out += "\nmins:" + str(mins)
    out += "\nnonZ:" + str(nonZ)
    return out


def main():
    s3 = boto3.resource("s3")
    files = ["Mirai_dataset.csv", "Scan_dataset.csv"]
    for file in files:
        obj = s3.Object("326-data-bucket", file)

        obj_dict = obj.get()
        obj_stream = obj_dict['Body']
        it = obj_stream.iter_lines(chunk_size=2048)

        avgs = []
        maxs = []
        mins = []
        nonZ = []

        count = 0
        for line in it:
            count+=1
            nums = line.decode(encoding='UTF-8').split(",")
            for ndx in range(1, len(nums)): 
                if count == 1:
                    avgs.append(float(nums[ndx]))
                    maxs.append(float(nums[ndx]))
                    mins.append(float(nums[ndx]))
                    if not float(nums[ndx]) == 0:
                        nonZ.append(1)
                    else:
                        nonZ.append(0)
                else:
                    i = ndx - 1
                    avgs[i] = (avgs[i] + float(nums[ndx]))/2
                    if float(nums[ndx]) > maxs[i]:
                        maxs[i] = float(nums[ndx])
                    if float(nums[ndx]) < mins[i]:
                        mins[i] = float(nums[ndx])
                    if not float(nums[ndx]) == 0:
                        nonZ[i]+=1 
        out = output(avgs, maxs, mins, nonZ, count)
        if file == "Mirai_dataset.csv":
            f = open("mirai_summary.txt", "w")
        else:
            f = open("scan_summary.txt", "w")
        f.write(out)
        f.close()


if __name__ == "__main__":
    main()
