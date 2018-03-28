from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import StandardScaler
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
    #label = d['AClose']
    label = d['PerMov']
    Features = getPriceArray(d['Child'][0])
    for vec in d['Child'][1:]:
        Features = Features + getPriceArray(vec)

    return (label,Features)

jsonData,ValData = sc.pickleFile(sys.argv[1]).\
    filter(lambda l:l['Symbol']!='SNP500').\
    filter(lambda l:len(l['Child']) == 30).randomSplit([80,20])

jsonData = jsonData.cache()
ValData = ValData.cache()

PriceData = jsonData.map(mapLabels).cache()

PriceFeatures = PriceData.map(lambda l:l[1])
scaler = StandardScaler().fit(PriceFeatures)


def addWordVec((f,d)):
    Features = d['Child'][0]['WordVec'].toArray().tolist()
    for vec in d['Child'][1:]:
        Features = Features + vec['WordVec'].toArray().tolist()
    return Vectors.dense(f + Features)


from pyspark.mllib.regression import LinearRegressionWithSGD

scaled_Pricefeatures = scaler.transform(PriceFeatures)
scaled_features = scaled_Pricefeatures.map(lambda  l:l.toArray().tolist()).\
                        zip(jsonData).\
                        map(addWordVec).cache()

Regdata = PriceData.map(lambda l:l[0]).zip(scaled_features).\
            map(lambda l:LabeledPoint(l[0],l[1])).cache()



mlpReg = LinearRegressionWithSGD.train(Regdata,
                                       iterations=100,
                                       step = 0.01,
                                       regType="l2",
                                       regParam=10,
                                       intercept=True)

def getRMSE(rdd):
    return rdd.map(lambda (p,l):(l['Symbol'],(1,(l['PerMov']-p)**2))).\
        reduceByKey(lambda v1,v2:(v1[0]+v2[0],v1[1]+v2[1])).\
        map(lambda (k,v):(k,float(v[1])/v[0])).collect()


#predTrain = mlpReg.predict(scaled_features).zip(jsonData).cache()
#print getRMSE(predTrain)

valFeatures = scaler.transform(ValData.map(mapLabels).map(lambda (a,b):b)).\
        map(lambda  l:l.toArray().tolist()).\
        zip(ValData).\
        map(addWordVec)
predVal = mlpReg.predict(valFeatures).zip(ValData).cache()
print "RMSE ",getRMSE(predVal)


mlpReg.save(sc,sys.argv[2])
