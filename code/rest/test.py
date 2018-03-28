import json
from pprint import pprint
import time, datetime

listDict = []

with open('part-00000.txt') as data_file:
	 
	 for row in data_file:
	 	sdict = {}
	 	data = json.loads(row)
	 	sdict['PredictionAClose'] = data['PredictionAClose']
	 	date = time.mktime(datetime.datetime.strptime(data['Date'], "%Y-%m-%d").timetuple())
	 	sdict['Date'] = date
	 	sdict['PerdictionDir'] = data['PerdictionDir']
	 	sdict['Dir'] = data['Dir']
	 	sdict['AClose'] = data['AClose']
	 	listDict.append(sdict)
	 pprint(json.dumps([dict(ndict=d) for d in listDict]))    