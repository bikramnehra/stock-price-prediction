from pyspark import SparkContext
from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("Portfolio")
sc = SparkContext(conf=conf)

def add(l1,l2):
    return map(lambda (a,b):a+b,zip(l1,l2))

#data = sc.pickleFile('./LabelsTrain')
data = sc.pickleFile(sys.argv[1])

std = data.map(lambda l:(l['Symbol'],[1.0,l['PerMov'],l['PerMov']**2])).\
        reduceByKey(add).\
        map(lambda (k,(c,ex,ex2)):(k,(ex/c,ex2/c))).\
        map(lambda (k,(ex,ex2)):(k,(ex2-ex**2)**0.5)).cache()


import matplotlib.pyplot as plt

PltArray = []

def plotData(data,name,companies):
    data = data.sortBy(lambda l:l).collect()
    start = 100
    for i in range(0,len(data)):
        start = start * data[i][1]
        data[i] = (data[i][0],start)
    key = map(lambda (k,v):k,data)
    val = map(lambda (k,v):v,data)
    p,= plt.plot(key,val,label=name)
    PltArray.append(p)
    print len(companies),len(key),len(val)
    return (key,val,name,companies)
    #plt.legend([p],[name])

def plotSNP500():
    #data = sc.pickleFile('./LabelsTest').\
    data = sc.pickleFile(sys.argv[4]).\
        filter(lambda d:d['Symbol'] == 'SNP500').\
        map(lambda d:(d['Date'],1+d['PerMov']*0.01)).\
        sortBy(lambda l:l).cache()
    dump = []
    plotData(data,"snp",dump)

def expandByDate((k,arr)):
    ret = []
    for i in arr:
        ret.append(((k,i[1][0]),i[0]))
    return ret

def getPerMov(d):
    previosPrice =  d['AClose']/(1+d['PerMov']*0.01)
    mov = (d['PredictionAClose'] - previosPrice)*100/previosPrice
    return mov
    #return d["PerMov"]

def getActualMov(d):
    return d["PerMov"]

import random
def getRandomMov(d):
    return random.random() - 1



def plotInvest(data,sk,riskIndex,Name,func):
    testdata = data.\
                map(lambda l:(l['Symbol'],l)).join(std).\
                map(lambda (k,v):v).\
                map(lambda (d,s):((d['Symbol'],d['Date']),(func(d),s))).\
                map(lambda (k,v):(k,v[0]-sk*riskIndex*(v[1]**2))).cache()
    scores = testdata.map(lambda (k,v):v).cache()
    maxScore = scores.max()
    minScore = scores.min()
    normData = testdata
    perAll = [33.0,26.0,20.0,13.0,8.0]

    investment = normData.map(lambda (k,v):(k[1],[(k[0],v)])).\
            reduceByKey(lambda c1,c2:c1+c2).\
            map(lambda (k,v):(k,sorted(v,key=lambda (s,v):-v)[0:5])).\
            map(lambda (k,v):(k,zip(perAll,v))).\
            sortBy(lambda l:l)

    companies = investment.map(lambda (k,v):map(lambda l:l[1][0],v)).collect()
            #flatMap(expandByDate)

    p = rawTest.map(lambda l:((l['Date'],l['Symbol']),l['PerMov'])).\
        join(investment.flatMap(expandByDate)).\
        map(lambda (k,v):(k[0],((1+v[0]*0.01)*v[1]))).\
        reduceByKey(lambda c1,c2:c1+c2).\
        map(lambda (k,v):(k,v/100)).\
        sortBy(lambda l:l)

    points = plotData(p,Name,companies)
    return points



import json
import datetime
PltArray = []
def mapDate(d):
    d['Date'] = datetime.datetime.strptime(d['Date'],"%Y/%m/%d")
    return d

#rawTest = sc.textFile('./Model3').\
rawTest = sc.textFile(sys.argv[2]).\
    map(lambda l:json.loads(l)).\
    map(mapDate).cache()

plotSNP500()
A = plotInvest(rawTest,3,1,"High Risk",getPerMov)

B = plotInvest(rawTest,5,1,"Medium Risk",getPerMov)
C = plotInvest(rawTest,10,1,"Low Risk",getPerMov)
#E = plotInvest(rawTest,5,1,"Actual Value",getActualMov)
D = plotInvest(rawTest,1,1,"Random",getRandomMov)
def mapPreviousValue((k,(d1,d2))):
    res = d1.copy()
    res['PerMov'] = d2['PerMov']
    return res

symbolDayId = rawTest.map(lambda l:((l['Symbol'],l['DayId']),l)).cache()
previousModel = symbolDayId.join(symbolDayId.map(lambda (k,v):((k[0],k[1]+1),v))).\
                map(mapPreviousValue)


F = plotInvest(previousModel,1,1,"Previous Value",getActualMov)
G = plotInvest(previousModel,5,1,"Previous Value Medium",getActualMov)


plt.legend(handles=PltArray,loc='upper left')
#plotInvest(rawTest,2,1)
plt.ylabel('Cumulative Return')
plt.xlabel('Time')
plt.title('Portfolio Comparison for Different Risk Values')
plt.show()


def toDict(l):
    ret = {}
    ret['Symbol'] = l[2]
    ret['Date'] = l[0].strftime("%Y/%m/%d")
    ret['Value'] = l[1]
    ret['Companies'] = l[3]
    return json.dumps(ret)

def storeAsRdd(col,name):
    return sc.parallelize(col).\
        map(toDict).\
    coalesce(1).saveAsTextFile(name)




result = zip(A[0],A[1],["HighRisk"]*len(A[0]),A[3])+\
          zip(B[0],B[1],["MediumRisk"]*len(B[0]),B[3]) +\
          zip(C[0],C[1],["LowRisk"]*len(C[0]),C[3])
dt = A[0][0]
storeAsRdd(result,sys.argv[3])


