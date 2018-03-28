import string
from pyspark.mllib.feature import Word2Vec,Word2VecModel
from pyspark import SparkContext
from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("Word2Vec")
sc = SparkContext(conf=conf)

def getPreProcessedWords(s):
    for punc in string.punctuation:
        s = s.replace(punc,' ')
        words = s.lower().split()
    return words

rawData = sc.pickleFile(sys.argv[1])

words = rawData.flatMap(lambda (k,v):v).\
        map(lambda v:v[1]).\
        map(getPreProcessedWords).cache()

word2vec = Word2Vec()
model = word2vec.fit(words)
model.save(sc,sys.argv[2])

res = model.getVectors()
keys = []
count = 0
for i in res:
    keys.append((i,model.transform(i)))
    count = count + 1

sc.parallelize(keys).\
    saveAsPickleFile(sys.argv[3])
