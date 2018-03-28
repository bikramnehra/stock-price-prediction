from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys
import numpy as np
from datetime import datetime
from operator import itemgetter, attrgetter, methodcaller
#import statistics 


conf = SparkConf().setAppName('Portfolio creation')
sc = SparkContext()
sqlContext = SQLContext(sc)
assert sc.version >= '1.5.1'

InputStockData = sys.argv[1] # /home/moon/Desktop/BigDataLAB-2/Project/stockPricePrediction/datadownload/stockData/

#User input between 1-5
riskIndex = 3 #sys.argv[3]

def flatData((k,v)):
    k = k.split('/')[-1]
    lines = v.splitlines()
    res = []
    for l in range(1,len(lines)):
        arr = lines[l].split(',')
        dt = datetime.strptime(arr[0], "%Y-%m-%d")
        di = {}
        di['Symbol'] = k
        di['Date'] = dt
        di['DayId'] = l
        di['Open'] = float(arr[1])
        di['High'] = float(arr[2])
        di['Low'] = float(arr[3])
        di['Close'] = float(arr[4])
        di['Volume'] = float(arr[5])
        di['AClose'] = float(arr[6])
        res.append((k,di))
    return res

prices = sc.wholeTextFiles(InputStockData).\
            flatMap(flatData).cache()
#print prices.take(1)

#***************************************************************************************************

current = prices.map(lambda (k,l):(k+"-"+str(l["DayId"]),l))
#print current.take(1)
prev = prices.map(lambda (k,l):(k+"-"+str(l["DayId"]-1),l))
#print prev.take(1)

def CalPriceMovement(k,arr):
    percentMov = ((arr[0]["AClose"] - arr[1]["AClose"])*100)/arr[1]["AClose"]
    arr[0]["PerMov"] = percentMov
    return (k,arr[0])

data = current.join(prev).\
        map(lambda (k,(a1,a2)):(k.split('-')[0],[a1,a2])).\
        map(lambda (k,arr):CalPriceMovement(k,arr))
#print data.take(1)


StandardDeviation = current.map(lambda (k,arr):(k.split('-')[0],(arr['AClose']))).\
    groupByKey().map(lambda x : ((x[0]), np.std(list(map(float,x[1])),axis = 0)))
#StandardDeviation = current.map(lambda (k,arr):(k.split('-')[0],(arr['AClose']))).\
#    groupByKey().map(lambda x : ((x[0]), statistics.pstdev(list(map(float,x[1])))))    

#print StandardDeviation.take(10)

combinedRDD = data.join(StandardDeviation).cache()
#print  combinedRDD.take(5)

ScalingFactor = 0.005

def UtilityScore(k,(arr,SD)):# User input between 1-5
    EV = arr['PerMov']*10
    #print EV
    score = (EV-(ScalingFactor*riskIndex*(SD*SD)))
    return (k,arr,abs(score))

utilityScoreRdd = combinedRDD.map(lambda (k,(arr,SD)) : UtilityScore(k,(arr,SD/100))).cache()
#print utilityScoreRdd.take(10)


def StockAllocate((stock,arr,score),R):
    return True
    if R == 1:
        if score > 75 and score < 100:
            return True
    elif R == 2:
        if score > 50 and score < 75:
            return True
    elif R == 3:
        if score > 25 and score < 50:
            return True
    elif R == 4:
        if score > 0 and score < 25:
            return True
    else:
        return False

StockAllocationRDD = utilityScoreRdd.filter(lambda (stock,arr,score):StockAllocate((stock,arr,score),riskIndex)).\
    map(lambda (k,arr,score):((arr['Date']),[(k,score)])).reduceByKey(lambda a, b: a + b)#.\
    #map(lambda (date,(stock,score)):(date,sorted(key=itemgetter(2))))

print StockAllocationRDD.take(1)

#StockPredictionList = sorted(StockAllocationRDD,key=itemgetter(2))
#print StockPredictionList



