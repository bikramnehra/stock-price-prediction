import sys
import json
import MySQLdb

fileName = sys.argv[1]
tableName = sys.argv[2]
dbUrl = sys.argv[3]
dbUser = sys.argv[4]
dbPass = sys.argv[5]
dbName = sys.argv[6]

db = MySQLdb.connect(dbUrl, dbUser, dbPass, dbName)
cursor = db.cursor()
cursor.execute("DROP TABLE IF EXISTS " + tableName)

sql = "CREATE TABLE " +tableName + """ (
Symbol  VARCHAR(100),
D DATE, 
value TEXT ,
PRIMARY KEY(Symbol,D))"""

cursor.execute(sql)

with open(fileName) as infile:
    for line in infile:
        di = json.loads(line)
        line = line.replace("'","")
        sql = "INSERT INTO "+tableName + " (Symbol,D,value) values ('" + str(di["Symbol"]) +"','" + di["Date"] +"','"+ line +"')"
        cursor.execute(sql)


db.commit()
db.close()
