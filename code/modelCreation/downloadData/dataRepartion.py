from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("data repartion")
sc = SparkContext(conf=conf)


sc.wholeTextFiles('file:////grad/1/jivjots/stockPricePrediction/datadownload/dataCopy',use_unicode=False).\
        repartition(1000).\
        saveAsPickleFile('file:////grad/1/jivjots/stockPricePrediction/datadownload/dataPickle3')
