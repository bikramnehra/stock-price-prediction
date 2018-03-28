from flask import Flask, jsonify, request
import json
import struct
import MySQLdb
import time, datetime

app = Flask(__name__)

dbUrl = sys.argv[1]
dbUser = sys.argv[2]
dbPass = sys.argv[3]
dbName = sys.argv[4]

db = MySQLdb.connect(dbUrl, dbUser, dbPass, dbName)
db.ping(True)
cursor = db.cursor()

def getStocks(tickr):
    listDict = []
    sql  = "SELECT value FROM stocks WHERE Symbol = "
    sql = sql + "'" + tickr + "';"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
           sdict = {}
           data = json.loads(row[0])
           sdict['pNews'] = data['P-News']
           sdict['pNewsRMSE'] = data['P-News-RMSE']
           sdict['pNewsPrices'] = data['P-News-Prices']
           sdict['pNewsPricesRMSE'] = data['P-News-Prices-RMSE']
           sdict['Date'] = data['Date']
           sdict['PredictionDir'] = data['PredictionDir']
           sdict['Dir'] = data['Dir']
           sdict['AClose'] = data['AClose']
           listDict.append(sdict)
        return json.dumps([d for d in listDict])
    except Exception,e:
        return str(e)

def getPortfolio():
    listDict = []
    sql  = "SELECT value FROM portfolio;"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
           pdict = {}
           data = json.loads(row[0])
           pdict['risk'] = data['Symbol']
           pdict['companies'] = data['Companies']
           pdict['date'] = data['Date']
           pdict['value'] = data['Value']
           listDict.append(pdict)
        return json.dumps([d for d in listDict])
    except Exception,e:
        return str(e)

@app.route('/stocks', methods=['GET'])
def get_stocks():
    tickr = request.args.getlist('tickr')
    data = getStocks(tickr[0])
    return data

@app.route('/portfolio', methods=['GET'])
def get_portfolio():
    data = getPortfolio()
    return data

