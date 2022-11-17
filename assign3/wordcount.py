"""
Authors: Marshall Jones and Scott Walters
nn.py
"""


def main():
   files = ["../datasets/numbers1.txt", "../datasets/numbers2.txt"]
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
