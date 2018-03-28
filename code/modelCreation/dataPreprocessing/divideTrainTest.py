from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("divide Train Test")
sc = SparkContext(conf=conf)
TestSet = [2015,2016]

sc.pickleFile(sys.argv[1]).\
        filter(lambda (k,v):k[1].year in TestSet).\
        saveAsPickleFile('TestNews')

sc.pickleFile('./ProcessedData/').\
        filter(lambda (k,v):k[1].year not in TestSet).\
        saveAsPickleFile('TrainNews')
