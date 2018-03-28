from xml.dom.minidom import parseString
from HTMLParser import HTMLParser
import re
import datetime

from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("clean Data")
sc = SparkContext(conf=conf)

# code copy start
# http://stackoverflow.com/questions/753052/strip-html-from-strings-in-python

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    data = s.get_data().replace('\n',' ')
    ret = u' '.join(data.split())
    return ret

# code copy finish

def getTitleAndDescription(item):
    title = item.getElementsByTagName('title')[0].firstChild.data
    title = strip_tags(title)
    description = item.getElementsByTagName('description')[0].firstChild.data
    description = strip_tags(description)
    pubDate = item.getElementsByTagName('pubDate')[0].firstChild.data
    pubDate = strip_tags(pubDate).split(" ")
    pubDate = '-'.join(pubDate[1:4])
    dt = datetime.datetime.strptime(pubDate, "%d-%b-%Y")
    return (dt,[(title,description)])

def getNewsList(xml):
    items = parseString(xml).getElementsByTagName('item')
    return map(getTitleAndDescription,items)

def flatNewsList((k,v)):
    arr = k.split('-')[1:]
    k = k.split('-')[0]
    v = map(lambda (d,arr):((k,d),arr),v)
    if len(v) == 0:
        dt = datetime.datetime.strptime('-'.join(arr), "%Y-%m-%d")+datetime.timedelta(days=-1)
        return [((k,dt),[('','')])]
    return v



rawData = sc.pickleFile(sys.argv[1]).\
            map(lambda (k,v):(k.split('/')[-1],v)).\
            map(lambda (k,v):(k.split('.')[0],getNewsList(v))).\
            flatMap(flatNewsList).\
            reduceByKey(lambda a1,a2:a1+a2).cache().\
            saveAsPickleFile(sys.argv[2])
