
fo = open("tickr.txt", "wb+")

with open('top100-Sheet1.csv', 'r') as datafile:
	for row in datafile:
		data = row.split(',')
		fo.write("\"{}\":\"{}\", ".format(data[0], data[1].title()))