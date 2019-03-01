from pyspark import SparkConf, SparkContext
import collections
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()
wordCounts = collections.OrderedDict(sorted(wordCounts.items()))
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord and count != 1):
        print(cleanWord.decode() + " " + str(count))
