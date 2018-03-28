import datetime
from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("extract labels")
sc = SparkContext(conf=conf)

def flatData((k,v)):
    k = k.split('/')[-1]
    lines = v.splitlines()
    res = []
    for l in range(1,len(lines)):
        arr = lines[l].split(',')
        dt = datetime.datetime.strptime(arr[0], "%Y-%m-%d")
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

def createPrevDate((k,(d,v))):
    date_1 = datetime.datetime.strptime(d, "%Y-%m-%d")
    end_date = date_1 + datetime.timedelta(days=-1)
    res = []
    res.append(k+"-"+d)
    res.append(k+"-"+end_date.strftime('%Y-%m-%d'))
    res.append(v)
    return res


prices = sc.wholeTextFiles(sys.argv[1]).\
        flatMap(flatData).cache()

current = prices.map(lambda (k,l):(k+"-"+str(l["DayId"]),l))

prev = prices.map(lambda (k,l):(k+"-"+str(l["DayId"]-1),l))

thresh = 0.5

def createLabels((k,arr),th):
    percentMov = ((arr[0]["AClose"] - arr[1]["AClose"])*100)/arr[1]["AClose"]
    arr[0]["PerMov"] = percentMov

    if percentMov >= th:
        arr[0]["Dir"] = 'up'
    elif percentMov <= -th:
        arr[0]["Dir"] = 'do'
    else:
        arr[0]["Dir"] = 'st'
    return (arr[0])

data = current.join(prev).\
        map(lambda (k,(a1,a2)):(k.split('-')[0],[a1,a2])).\
        map(lambda l:createLabels(l,thresh))


TestSet = range(2015,2017)
TrainSet = range(2010,2015)

data.filter(lambda v:v['Date'].year in TrainSet).\
        saveAsPickleFile('LabelsTrain')

data.filter(lambda v:v['Date'].year in TestSet).\
        saveAsPickleFile('LabelsTest')
