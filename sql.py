import csv
import sys
import re
from collections import OrderedDict

def main():
	dictionary = {}
	readMetadata(dictionary)
        da=str(sys.argv[1])
	processQuery(da,dictionary)

def readMetadata(dictionary):
	flag=0
	f = open('./metadata.txt','r')
	table=[]
	check = 0
	for line in f:
		if line.strip() == "<begin_table>":
			table.append(line.strip())
			check = 1
			continue
		if check == 1:
			dictionary[line.strip()] = [];
			check = 0
			tableName=line.strip()
			continue
		if line.strip()=='<end_table>':
			table.append(tableName)
			table.append(line.strip())
		if not line.strip() == '<end_table>':
			re=line.strip()
			dictionary[tableName].append(re)		

def processQuery(query,dictionary):
	query = (re.sub(' +',' ',query))
	flag1=0
	tab=[]
	var=[]
	col=[]
	#print query
	query=query.strip()
		
	if "from" in query:
		flag1=1
		obj1 = query.split('from');
		tab.append(obj1[1])
	if "from" not in query:
		flag1=0
		sys.exit("Incorrect Syntax")

	obj1[0] = (re.sub(' +',' ',obj1[0])).strip();
	temp2=obj1[0]
	befrom=obj1[0].lower()
	if "select" not in befrom:
		sys.exit("Incorrect Syntax")
	object1 = obj1[0][7:]
	var.append(object1)
	object1 = (re.sub(' +',' ',object1)).strip();
	l = []
	l.append("select")

	if "distinct" in object1: 
		if "distinct(" not in object1:
			object1 = object1[9:]
			var.append(object1)
			l.append("distinct")

	l.append(object1)
	obj45=[]
	object1 = l 
	var.append(l)
	object3 = ""
	if "distinct" in object1[1] and "distinct(" not in object1[1]:
		var.append(object3)
		object3 = object1[1]
		object3 = (re.sub(' +',' ',object3))
		object3=object3.strip()
		temp=object1[1]
		object1[1] = object1[2]

	colStr = object1[1]
	colStr = (re.sub(' +',' ',colStr))
	sf=colStr
	colStr=colStr.strip()
	columnNames = colStr.split(',')
	var.append(colStr.strip())
	for i in columnNames:
		iu=columnNames.index(i)
		columnNames[iu] = (re.sub(' +',' ',i)).strip();
		col.append(i)

	obj1[1] = (re.sub(' +',' ',obj1[1])).strip();
	var.append(obj1[1])
	object2 = obj1[1].split('where');
	tableStr = object2[0]
	tab.append(tableStr)
	tableStr = (re.sub(' +',' ',tableStr)).strip();
	tableNames = tableStr.split(',')
	for i in tableNames:
		uri=tableNames.index(i)
		tableNames[uri] = (re.sub(' +',' ',i)).strip();
		var.append(uri)
	for i in tableNames:
		if i not in dictionary.keys():
			flag2=0
			sys.exit("Table not found")

	if len(object2) > 1 and len(tableNames) == 1:
		rt=len(object2)
		object2[1] = (re.sub(' +',' ',object2[1])).strip();
		var.append(object2[1])
		processWhere(object2[1],columnNames,tableNames,dictionary)
		return
	elif len(object2) > 1 :
		if len(tableNames) > 1:
			object2[1] = (re.sub(' +',' ',object2[1])).strip();
			tab.append(object2[1])
			processWhereJoin(object2[1],columnNames,tableNames,dictionary)
			return

	if(len	(tableNames) > 1):
		nooftable=len(tableNames)
		join(columnNames,tableNames,dictionary)
		return

	if object3 == "distinct":
		flag4=1
		distinctMany(columnNames,tableNames,dictionary)
		return
	
	if len(columnNames) == 1:
		
		for col in columnNames:
			if '(' in col and ')' in col:
				fa=col.split(')')
				funcName = ""
				var.append(fa[0])
				colName = ""
				a1 = col.split('(');
				funcName = (re.sub(' +',' ',a1[0]))
				funcName=funcName.strip()
				colName = (re.sub(' +',' ',a1[1].split(')')[0]))
				colName=colName.strip()
				aggregate(funcName,colName,tableNames[0],dictionary)
				return
			elif '(' in col or ')' in col:
				flag3=0
				sys.exit("error in syntax")
	#col3=col.strip()
	selectColumns(columnNames,tableNames,dictionary);

def processWhere(whereStr,columnNames,tableNames,dictionary):
	a = whereStr.split(" ")
	fil=[]
	col=[]
	# print a

	if(len(columnNames) == 1 and columnNames[0] == '*'):
		col.append(columnNames[1])
		columnNames = dictionary[tableNames[0]]

	printHeader(columnNames,tableNames,dictionary)
	chec3=0
	tName = tableNames[0] + '.csv'
	fileData = []
	fil.append(tName)
	readFile(tName,fileData)

	check = 0
	for data in fileData:
		string = evaluate(a,tableNames,dictionary,data)
		for col in columnNames:
			a=col.split(',')
			if eval(string):
				check = 1
				chec3=0
				print data[dictionary[tableNames[0]].index(col)],
		if check == 1:
			check = 0
			chec3=1
			print

def evaluate(a,tableNames,dictionary,data):
	string = ""
	cn=0
	for i in a:
		if i == '=':
			string += i*2
			cn=1
		elif i in dictionary[tableNames[0]] :
			cn=2
			string += data[dictionary[tableNames[0]].index(i)]
			temp=dictionary[tableNames[0]]
		elif i.lower() == 'and' or i.lower() == 'or':
			cn=3
			string += ' ' + i.lower() + ' '
		else:
			cn=4
			string += i
	
	return string

def processWhereJoin(whereStr,columnNames,tableNames,dictionary):
	tmep=[]	
	tableNames.reverse()
	a3=[]
	l1 = []
	l2 = []
	readFile(tableNames[0] + '.csv',l1)
	fljoin=1
	readFile(tableNames[1] + '.csv',l2)

	fileData = []
	for item1 in l1:
		for item2 in l2:
			st3=item2 + item1
			fileData.append(st3)

	
	dictionary["sample"] = []
	for i in dictionary[tableNames[1]]:
		a3.append(i)
		dictionary["sample"].append(tableNames[1] + '.' + i)
	for i in dictionary[tableNames[0]]:
		a3.append(i)
		dictionary["sample"].append(tableNames[0] + '.' + i)
	
	c3=dictionary[tableNames[1]] + dictionary[tableNames[0]]
	dictionary["test"] = c3
	tableNames.remove(tableNames[0])
	fljoin=0
	tableNames.remove(tableNames[0])
	a3.append("test")
	tableNames.insert(0,"sample")

	if(len(columnNames) == 1 ):
		if columnNames[0] == '*':
			a3.append(columnNames)
			columnNames = dictionary[tableNames[0]]

	ter=len(columnNames)
	for i in columnNames:
		print i,
	print

	a = whereStr.split(" ")

	ch3=0
	check = 0
	for data in fileData:
		ch3=1
		string = evaluate(a,tableNames,dictionary,data)
		for col in columnNames:
			if eval(string):
				ch3=0
				check = 1
				if '.' in col:
					st='.'
					b=dictionary[tableNames[0]].index(col)
					print data[b],
				else:
					c=dictionary["test"].index(col)
					print data[c],
		if check == 1:
			check = 0
			a3.append(col)
			print

	del dictionary['sample']

def selectColumns(columnNames,tableNames,dictionary):
	col=[]

	if len(columnNames) == 1 :
		if columnNames[0] == '*':
			col.append(columnNames)
			columnNames = dictionary[tableNames[0]]

	for i in columnNames:
		if i not in dictionary[tableNames[0]]:
			fla4=0
			sys.exit("error")
		if i in dictionary[tableNames[0]]:
			fla4=1

	printHeader(columnNames,tableNames,dictionary)
	ext='.csv'
	tName = tableNames[0] + ext
	fileData = []
	col.append(tName)
	readFile(tName,fileData)
	
	printData(fileData,columnNames,tableNames,dictionary)

def aggregate(func,columnName,tableName,dictionary):
	ext='.csv'
	tab=[]
	if columnName == '*':
		fla=0
		sys.exit("error")
	if columnName not in dictionary[tableName]:
		fla1=0
		sys.exit("error")
	if columnName in dictionary[tableName]:
		fla1=1

	tName = tableName + ext
	fileData = []
	readFile(tName,fileData)
	tab.append(tName)
	colList = []
	for data in fileData:
		tab.append(data)
		colList.append(int(data[dictionary[tableName].index(columnName)]))

	if func.lower() == 'max':
		tab.append(func)
		print max(colList)
	elif func.lower() == 'min':
		tab.append(func)
		print min(colList)
	elif func.lower() == 'sum':
		tab.append(func)
		print sum(colList)
	elif func.lower() == 'avg':
		tab.append(func)
		print sum(colList)/len(colList)
	elif func.lower() == 'distinct':
		tab.append(func)
		distinct(colList,columnName,tableName,dictionary);
	else :
		fla=0
		print "ERROR"
		print "Unknown function : ", '"' + func + '"'

def distinct(colList,columnName,tableName,dictionary):
	col2=[]
	print "OUTPUT :"
	rr=tableName + '.'
	string =  rr+ columnName
	x=len(string)
	print string
	
	colList = list(OrderedDict.fromkeys(colList))
	col2.append(colList)
	for col in range(len(colList)):
		x=colList[col]
		print x

def distinctMany(columnNames,tableNames,dictionary):
	printHeader(columnNames,tableNames,dictionary)
	ext=".csv"
	temp = []
	tem1=[]
	check = 0
	for tab in tableNames:
		tName = tab + '.csv'
		tem1.append(tab)
		with open(tName,'rb') as f:
			reader = csv.reader(f)
			flag=1
			for row in reader:
				for col in columnNames:
					a=dictionary[tableNames[0]].index(col)
					x = row[a]
					if x not in temp:
						temp.append(x)
						check =1
						print x,
					if x in temp:
						tem1.append(x)
				if check == 1 :
					flag=0
					check = 0
					print

def join(columnNames,tableNames,dictionary):
	tableNames.reverse()
	a3=[]
	l1 = []
	l2 = []
	tab=[]
	a=tableNames[0] + '.csv'
	readFile(a,l1)
	b=tableNames[1] + '.csv'
	readFile(b,l2)

	fileData = []
	for item1 in l1:
		for item2 in l2:
			ty=item2 + item1
			fileData.append(ty)

	
	dictionary["sample"] = []
	fl1=1
	for i in dictionary[tableNames[1]]:
		a3.append(i)
		dictionary["sample"].append(tableNames[1] + '.' + i)
	for i in dictionary[tableNames[0]]:
		tab.append(tableNames[0])	
		dictionary["sample"].append(tableNames[0] + '.' + i)
	a3.append("test")
	a5=dictionary[tableNames[1]] + dictionary[tableNames[0]]
	dictionary["test"] = a5
	

	tableNames.remove(tableNames[0])
	tab.append(tableNames)
	tableNames.remove(tableNames[0])
	flag=1
	tableNames.insert(0,"sample")

	if(len(columnNames) == 1): 
		if columnNames[0] == '*':
			columnNames = dictionary[tableNames[0]]
			tab.append(columnNames)


	for i in columnNames:
		print i,
	print



	for data in fileData:
		for col in columnNames:
			a3.append(col)
			if '.' in col:
				a=dictionary[tableNames[0]].index(col)
				print data[a],
			else:
				b=dictionary["test"].index(col)
				print data[b],
		print



def printHeader(columnNames,tableNames,dictionary):
	
	print "OUTPUT : "
	col1=[]
	string = ""
	for col in columnNames:
		for tab in tableNames:
			if col in dictionary[tab]:
				if not string == "":
					flag=1
					string += ','
				if string== "":
					col1.append(col)
				string += tab + '.' + col
	print string

def printData(fileData,columnNames,tableNames,dictionary):
	temp=[]	
	for data in fileData:
		for col in columnNames:
			a=dictionary[tableNames[0]].index(col)
			print data[a],
			temp.append(a)			
		print

def readFile(tName,fileData):
	with open(tName,'rb') as f:
		reader = csv.reader(f)
		for row in reader:
			fileData.append(row)

if __name__ == "__main__":
	main()
