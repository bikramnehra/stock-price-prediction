from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import StandardScaler
import json
import sys

conf = SparkConf().setAppName("TrainWord2VecAvgFeatures.py")
sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

def mapLabel(st):
    if st == 'up':
        return 0.0
    elif st == 'do':
        return 1.0
    elif st == 'st':
        return 2.0
    else:
        return 3.0


def getPriceArray(di):
    ret = []
    ret.append(di["Volume"])
    ret.append(di["High"])
    ret.append(di["Low"])
    ret.append(di["PerMov"])
    ret.append(di["Close"])
    ret.append(di["Open"])
    ret.append(di["AClose"])
    #ret.append(di['WordVec'])
    return ret

def mapLabels(d):
    label = d['AClose']
    Features = getPriceArray(d['Child'][0])
    for vec in d['Child'][1:]:
        Features = Features + getPriceArray(vec)

    return (label,Features)

jsonData = sc.pickleFile(sys.argv[1]).\
    filter(lambda l:l['Symbol']!='SNP500').\
    filter(lambda l:len(l['Child']) == 30)#.randomSplit([80,20])

jsonData = jsonData.cache()

TestData = sc.pickleFile(sys.argv[2]).\
        filter(lambda l:l['Symbol']!='SNP500').\
        filter(lambda l:len(l['Child']) == 30).cache()#.randomSplit([80,20])


PriceData = jsonData.map(mapLabels).cache()

PriceFeatures = PriceData.map(lambda l:l[1])
scaler = StandardScaler().fit(PriceFeatures)


def addWordVec((f,d)):
    Features = d['Child'][0]['WordVec'].toArray().tolist()
    for vec in d['Child'][1:]:
        Features = Features + vec['WordVec'].toArray().tolist()
    return Vectors.dense(f + Features)


from pyspark.mllib.regression import LinearRegressionWithSGD, LinearRegressionModel

scaled_Pricefeatures = scaler.transform(PriceFeatures)
scaled_features = scaled_Pricefeatures.map(lambda  l:l.toArray().tolist()).\
                        zip(jsonData).\
                        map(addWordVec).cache()

Regdata = PriceData.map(lambda l:l[0]).zip(scaled_features).\
            map(lambda l:LabeledPoint(l[0],l[1])).cache()



#mlpReg = LinearRegressionWithSGD.train(Regdata,
#                                       iterations=100,
#                                       step = 0.01,
#                                       regType="l2",
#                                       regParam=1,
#                                       intercept=True)

mlpReg = LinearRegressionModel.load(sc,sys.argv[3])

def getRMSEValue(p,l):
    return (p -l['AClose'])**2


def getRMSE(rdd):
    return rdd.map(lambda (p,l):(1,(1,getRMSEValue(p,l)))).\
        reduceByKey(lambda v1,v2:(v1[0]+v2[0],v1[1]+v2[1])).\
        map(lambda (k,v):(k,(float(v[1])/v[0])**0.5)).collect()


#predTrain = mlpReg.predict(scaled_features).zip(jsonData).cache()
#print "RMSE: ",getRMSE(predTrain)

TestFeatures = scaler.transform(TestData.map(mapLabels).map(lambda (a,b):b)).\
        map(lambda  l:l.toArray().tolist()).\
        zip(TestData).\
        map(addWordVec)

predVal = mlpReg.predict(TestFeatures).zip(TestData).cache()

print "RMSE: ",getRMSE(predVal)

def mapResult((p,d)):
    res = d.copy()
    res.pop('Child')
    res['PredictionAClose'] = p
    res['PredictionDir'] = 'up'
    res['Date'] = res['Date'].strftime('%Y/%m/%d')
    return json.dumps(res)


predVal.map(mapResult).\
        saveAsTextFile(sys.argv[4])

