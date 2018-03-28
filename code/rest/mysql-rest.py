from flask import Flask, jsonify, request
import json
import struct
import MySQLdb

app = Flask(__name__)

db = MySQLdb.connect("bikramnehra.mysql.pythonanywhere-services.com","bikramnehra","mysqlpass","bikramnehra$default" )
cursor = db.cursor()

def getStocks(tags):
    sql  = "SELECT value FROM stock LIMIT 10;"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            key = row[0]
            data = row[1]
            temp = json.loads(data)
        return temp;
    except Exception,e:
        print str(e)


@app.route('/stocks', methods=['GET'])
def get_tasks():
    tags = request.args.getlist('tag')
    data = getStocks(tags)
    return data

if __name__ == '__main__':
    app.run(host= 'bikramnehra.mysql.pythonanywhere-services.com',port=80)
