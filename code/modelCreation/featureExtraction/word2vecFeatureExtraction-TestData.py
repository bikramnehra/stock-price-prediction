import string
import datetime
from pyspark.sql.types import StructType,StructField,StringType,DateType,FloatType,IntegerType,ArrayType
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
import copy
from pyspark.mllib.linalg import DenseVector
from pyspark import SparkContext
from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("word2vecFeatureExtraction")
sc = SparkContext(conf=conf)


stop_words = set(sc.pickleFile(sys.argv[1]).collect())

def flatten((k,v)):
    res = []
    for item in v:
        res.append((k,item))
    return res

def getPreProcessedWords(s):
    for punc in string.punctuation:
        s = s.replace(punc,' ')
    words = s.lower().split()
    ret = []
    for w in words:
        if w not in stop_words:
            ret.append(w)
    return ret

wordVecDict = dict(sc.pickleFile(sys.argv[2]).collect())





def mapFeatures((k,v)):
    res = []
    for word in v:
        if(word in  wordVecDict):
            res.append(wordVecDict[word])
    if len(res) != 0:
        return (k,(1,sum(res)/len(res)))
    else:
        return (k,(1,DenseVector([0]*100)))

data = sc.pickleFile(sys.argv[3]).\
            filter(lambda (k,v):k[1].year == 2014).\
            union(sc.pickleFile(sys.argv[4])).\
            flatMap(flatten).\
            map(lambda (k,v):(k,getPreProcessedWords(v[1]))).cache()


wordVecFeatures = data.map(mapFeatures).\
                    reduceByKey(lambda c1,c2:(c1[0]+c2[0],c1[1]+c2[1])).\
                    cache()

from pyspark.sql.types import StructType,StructField,StringType,DateType,FloatType,IntegerType,ArrayType
import datetime

window = 30
def mapDates(v):
    l = k.split("-")
    d = datetime.datetime.strptime("-".join(l[1:]),"%Y-%m-%d")
    ret = {}
    ret['Symbol'] = l[0]
    ret['Date'] = d
    ret['PercentMovement'] = v[0]
    ret['Price'] = v[1]
    ret['DayId'] = v[2]
    ret['Direction'] = v[3]
    return ret

schema = StructType([   StructField("Symbol",StringType()),
                        StructField("Date",DateType()),
                        StructField("PercentMovement",FloatType()),
                        StructField("Price",FloatType()),
                        StructField("DayId",IntegerType()),
                        StructField("Direction",StringType())])


rdd = sc.pickleFile(sys.argv[5]).\
        filter(lambda l:l['Date'].year == 2014).\
        union(sc.pickleFile(sys.argv[6])).\
        cache()

rddKey = rdd.map(lambda l:((l['Symbol'],l['DayId']),l))

def flatMapRange(l):
    r = range(l['DayId']+1,l['DayId']+window+1)
    ret =  []
    for i in r:
        ret.append(((l['Symbol'],i),l))
    return ret

def mapAggKey((k,v)):
    act = v[0].copy()
    act['Last-T'] = [v[1].copy()]
    return ((act['Symbol'],act['DayId']),act)

def reduceAggKey(v1,v2):
    cp = v1.copy()
    cp['Last-T'] = v1['Last-T'] + v2['Last-T']
    return cp

def sortAggKey((k,v)):
    cp = v.copy()
    cp['Last-T'] = sorted(v['Last-T'],key = lambda l:l['DayId'])
    return cp

def getDateRange(start,end):
    ret = []
    while start < end:
        ret.append(start)
        start = start + datetime.timedelta(days=1)
    return ret
    #break

def mapRevDate(di):
    res = []
    childs = di['Last-T']
    di.pop('Last-T')
    #childs[0]['Date']['Parent'] = di
    childs[0]['Parent'] = di.copy()
    for da in  getDateRange(childs[0]['Date'],childs[0]['Parent']['Date']):
            res.append(((childs[0]['Symbol'],da),childs[0].copy()))
    for d in range(1,len(childs)):
        #d['Parent'] = di
        childs[d]['Parent'] = di.copy()
        for da in  getDateRange(childs[d]['Date'],childs[d-1]['Date']):
            res.append(((childs[d]['Symbol'],da),childs[d].copy()))
    return res

TestSet = [2015,2016]

rddRange = rdd.flatMap(flatMapRange).\
            join(rddKey).\
            map(mapAggKey).\
            reduceByKey(reduceAggKey).\
            map(sortAggKey).\
            flatMap(mapRevDate).cache()


def mapWord2VecJoin((k,(di,wo))):
    di['WordVec'] = wo
    key = (di['Symbol'],di['Date'],di['Parent']['Date'])
    return (key,di)

def reduceWord2VecJoin(d1,d2):
    c = d1['WordVec'][0] + d2['WordVec'][0]
    vec = d1['WordVec'][1] + d2['WordVec'][1]
    d1['WordVec'] = (c,vec)
    return d1

def rddRangeJoinAvg((k,d)):
    avg = d['WordVec'][1] / d['WordVec'][0]
    parent = d['Parent']
    d['WordVec'] = avg
    d.pop('Parent')
    parent['Child'] = [d]
    key = (parent['Symbol'],parent['Date'])
    return (key,parent)

def reduceAppendChild(d1,d2):
    d1['Child'] = d1['Child'] + d2['Child']
    return d1

def sortChildren((k,d)):
    d['Child'] = sorted(d['Child'],key=lambda l:l['DayId'])
    return (d)

finalData = rddRange.join(wordVecFeatures).\
        map(mapWord2VecJoin).\
        reduceByKey(reduceWord2VecJoin).\
        map(rddRangeJoinAvg).\
        reduceByKey(reduceAppendChild).\
        map(sortChildren).\
        filter(lambda l:l['Date'].year in TestSet)


finalData.saveAsPickleFile(sys.argv[7])
