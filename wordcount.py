'''
Authors: Marshall Jones and Scott Walters
Wordcount.py
'''

def removePunct(words):
    punc = ['.','-','\'','(',')','\"',',','!','?','&','\\','$','/','+','*','@', '~', '`']
    lower = words.lower()
    for ele in lower:
        if ele in punc:
            lower.replace(ele, "")
    print(lower)
    return lower

def main():
    files = ["data2/article_text.txt"]
    for f in range(0, len(files)):
        file = open(files[f], 'r')
        passage = file.read()
        file.close()

        passage = removePunct(passage)

        output = open("summary_" + str(f+1) + ".txt", "w")
        data = 0
        output.write(data)
        output.close()

if __name__ == "__main__":
    main()