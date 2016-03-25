import psycopg2
import sys
import csv
import os

try:
	con = psycopg2.connect(database='postgres', user=os.environ['USER'], host="/home/"+os.environ['USER']+"/postgres")
	cur = con.cursor()

except psycopg2.DatabaseError, e:
	print 'Error %s' % e
	sys.exit(1)

#re-create tables if they exist so need to drop
cur.execute("DROP TABLE IF EXISTS electric_data;")
cur.execute("DROP TABLE IF EXISTS electric_misc;")
cur.execute("DROP TABLE IF EXISTS transport_data;")
cur.execute("DROP TABLE IF EXISTS transport_misc;")
cur.execute("DROP TABLE IF EXISTS mkwh_data;")
cur.execute("DROP TABLE IF EXISTS mkwh_misc;")
#create tables
cur.execute(
	"CREATE TABLE electric_misc(col INT PRIMARY KEY NOT NULL, \
	msn VARCHAR(10), description VARCHAR(100), unit VARCHAR(50));")
cur.execute(
	"CREATE TABLE electric_data(col INT, date INT, value FLOAT);")
cur.execute(
	"CREATE TABLE transport_misc(col INT PRIMARY KEY NOT NULL, \
	msn VARCHAR(10), description VARCHAR(100), unit VARCHAR(50));")
cur.execute(
	"CREATE TABLE transport_data(col INT, date INT, value FLOAT);")
cur.execute(
	"CREATE TABLE mkwh_misc(col INT PRIMARY KEY NOT NULL, \
	msn VARCHAR(10), description VARCHAR(100), unit VARCHAR(50));")
cur.execute(
	"CREATE TABLE mkwh_data(col INT, date INT, value FLOAT);")

#opens and parses csv file
#saves data into array/list

#insert electricity data
count = 0
f = open("/home/cjnitta/ecs165a/EIA_CO2_Electricity_2015.csv", 'rb')
msnData = []
descriptionData = []
columnData = []
unitData = []

reader = csv.reader(f)
#gets the column names
fields = next(reader)
#gets first set of data to initialize 
first = next(reader)
msnData.append(first[0])
columnData.append(first[3])
descriptionData.append(first[4])
unitData.append(first[5])
if(first[2] == "Not Available"):
	queryString = "INSERT INTO electric_data VALUES (%s, %s, %s);" \
	%(first[3], first[1], "-1")
	cur.execute(queryString)
else:
	queryString = "INSERT INTO electric_data VALUES (%s, %s, %s);" \
	%(first[3], first[1], first[2])
	cur.execute(queryString)

for row in reader:
	if (row[0] != msnData[count]): 	
	#if msn does not exit in table yet
	#increment index of msn array for next check
		msnData.append(row[0])
		columnData.append(row[3])		
		descriptionData.append(row[4])
		unitData.append(row[5])
		count += 1	
	if(row[2] == "Not Available"):
		queryString = "INSERT INTO electric_data VALUES (%s, %s, %s);" \
		%(row[3], row[1], "-1")
		cur.execute(queryString)
	else:
		queryString = "INSERT INTO electric_data VALUES (%s, %s, %s);" \
		%(row[3], row[1], row[2])
		cur.execute(queryString)

for i in range(len(msnData)):
	queryString = "INSERT INTO electric_misc VALUES (%s, '%s', '%s', '%s');" \
	%(columnData[i], msnData[i], descriptionData[i], unitData[i])
	cur.execute(queryString)	

#insert transportation data
count = 0
f = open("/home/cjnitta/ecs165a/EIA_CO2_Transportation_2015.csv", 'rb')
msnData = []
descriptionData = []
columnData = []
unitData = []

reader = csv.reader(f)
#gets the column names
fields = next(reader)
#gets first set of data to initialize 
first = next(reader)
msnData.append(first[0])
columnData.append(first[3])
descriptionData.append(first[4])
unitData.append(first[5])
if(first[2] == "Not Available"):
	queryString = "INSERT INTO transport_data VALUES (%s, %s, %s);" \
	%(first[3], first[1], "-1")
	cur.execute(queryString)
else:
	queryString = "INSERT INTO transport_data VALUES (%s, %s, %s);" \
	%(first[3], first[1], first[2])
	cur.execute(queryString)

for row in reader:
	if (row[0] != msnData[count]): 	
	#if msn does not exit in table yet
	#increment index of msn array for next check
		msnData.append(row[0])
		columnData.append(row[3])		
		descriptionData.append(row[4])
		unitData.append(row[5])
		count += 1	
	if(row[2] == "Not Available"):
		queryString = "INSERT INTO transport_data VALUES (%s, %s, %s);" \
		%(row[3], row[1], "-1")
		cur.execute(queryString)
	else:
		queryString = "INSERT INTO transport_data VALUES (%s, %s, %s);" \
		%(row[3], row[1], row[2])
		cur.execute(queryString)

for i in range(len(msnData)):
	queryString = "INSERT INTO transport_misc VALUES (%s, '%s', '%s', '%s');" \
	%(columnData[i], msnData[i], descriptionData[i], unitData[i])
	cur.execute(queryString)

#insert mkwh data
count = 0
f = open("/home/cjnitta/ecs165a/EIA_MkWh_2015.csv", 'rb')
msnData = []
descriptionData = []
columnData = []
unitData = []

reader = csv.reader(f)
#gets the column names
fields = next(reader)
#gets first set of data to initialize 
first = next(reader)
msnData.append(first[0])
columnData.append(first[3])
descriptionData.append(first[4])
unitData.append(first[5])
if(first[2] == "Not Available"):
	queryString = "INSERT INTO mkwh_data VALUES (%s, %s, %s);" \
	%(first[3], first[1], "-1")
	cur.execute(queryString)
else:
	queryString = "INSERT INTO mkwh_data VALUES (%s, %s, %s);" \
	%(first[3], first[1], first[2])
	cur.execute(queryString)

for row in reader:
	if (row[0] != msnData[count]): 	
	#if msn does not exit in table yet
	#increment index of msn array for next check
		msnData.append(row[0])
		columnData.append(row[3])		
		descriptionData.append(row[4])
		unitData.append(row[5])
		count += 1	
	if(row[2] == "Not Available"):
		queryString = "INSERT INTO mkwh_data VALUES (%s, %s, %s);" \
		%(row[3], row[1], "-1")
		cur.execute(queryString)
	else:
		queryString = "INSERT INTO mkwh_data VALUES (%s, %s, %s);" \
		%(row[3], row[1], row[2])
		cur.execute(queryString)

for i in range(len(msnData)):
	queryString = "INSERT INTO mkwh_misc VALUES (%s, '%s', '%s', '%s');" \
	%(columnData[i], msnData[i], descriptionData[i], unitData[i])
	cur.execute(queryString)

###################################################################################################
    
cur.execute("DROP TABLE IF EXISTS day_table;")
cur.execute("DROP TABLE IF EXISTS hh_table;")
cur.execute("DROP TABLE IF EXISTS veh_table;")
cur.execute("DROP TABLE IF EXISTS person_table;")

#####Day Trip File
queryString=""
f = open("/home/cjnitta/ecs165a/DAYV2PUB.CSV", 'rb')
reader = csv.reader(f)
heading = next(reader)
queryString = "CREATE TABLE day_table(%s INT, %s INT, %s INT, %s INT, %s INT, %s INT, %s INT, %s INT, %s FLOAT);" \
%(heading[0], heading[1], heading[28], heading[38], heading[64], heading[83], heading[91], heading[93], heading[94])
cur.execute(queryString)

count = 0
queryString=""
for row in reader: 
	if count == 1500:
		count = 0
		cur.execute(queryString)
		queryString=""
	queryString+= "INSERT INTO day_table VALUES(%s, %s, '%s', %s, %s, %s, %s, %s, %s);" \
	%(row[0], row[1], row[28], row[38], row[64], row[83], row[91], row[93], row[94])
	count+= 1
if queryString != "": 
	cur.execute(queryString)


#####Household File
queryString=""
f = open("/home/cjnitta/ecs165a/HHV2PUB.CSV", 'rb')
reader = csv.reader(f)
heading = next(reader)
queryString = "CREATE TABLE hh_table(%s INT, %s INT, %s INT, %s INT);" \
%(heading[0], heading[3], heading[12], heading[15])
cur.execute(queryString)

count = 0
queryString=""
for row in reader: 
	if count == 1500:
		count = 0
		cur.execute(queryString)
		queryString=""
	queryString+= "INSERT INTO hh_table VALUES(%s, %s, %s, %s);" \
	%(row[0], row[3], row[12], row[15])
	count+= 1
if queryString != "": 
	cur.execute(queryString)

#####Person File
queryString=""
f = open("/home/cjnitta/ecs165a/PERV2PUB.CSV", 'rb')
reader = csv.reader(f)
heading = next(reader)
queryString = "CREATE TABLE person_table(%s INT, %s INT, %s INT, %s INT, %s FLOAT);" \
%(heading[0], heading[1], heading[7], heading[10], heading[100])
cur.execute(queryString)

count = 0
queryString=""
for row in reader: 
	if count == 1500:
		count = 0
		cur.execute(queryString)
		queryString=""
	queryString+= "INSERT INTO person_table VALUES(%s, %s, %s, %s, %s);" \
	%(row[0], row[1], row[7], row[10], row[100])
	count+= 1
if queryString != "": 
	cur.execute(queryString)


#####Vehicle File
queryString=""
f = open("/home/cjnitta/ecs165a/VEHV2PUB.CSV", 'rb')
reader = csv.reader(f)
heading = next(reader)
queryString = "CREATE TABLE veh_table(%s INT, %s INT, %s INT, %s INT, %s FLOAT, %s INT, %s FLOAT, %s FLOAT, %s INT);" \
%(heading[0], heading[2], heading[3], heading[6], heading[38], heading[51], heading[55], heading[57], heading[58])
cur.execute(queryString)

count = 0
queryString=""
for row in reader: 
	if count == 1500:
		count = 0
		cur.execute(queryString)
		queryString=""
	queryString+= "INSERT INTO veh_table VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);" \
	%(row[0], row[2], row[3], row[6], row[38], row[51], row[55], row[57], row[58])
	count+= 1
if queryString != "": 
	cur.execute(queryString)
	

con.commit()
#close file
f.close()
#close database connection
con.close()
