import sys
import json
import MySQLdb

fileName = sys.argv[1]
tableName = sys.argv[2]


#db = MySQLdb.connect("127.0.0.1","testuser","password","testdb" )
db = MySQLdb.connect("bikramnehra.mysql.pythonanywhere-services.com","bikramnehra","mysqlpass","bikramnehra$default" )
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
        #print di
        line = line.replace("'","")
        #print line
        sql = "INSERT INTO "+tableName + " (Symbol,D,value) values ('" + str(di["Symbol"]) +"','" + di["Date"] +"','"+ line +"')"
        #print sql
        cursor.execute(sql)
        #sys.exit(1)


db.commit()
db.close()
