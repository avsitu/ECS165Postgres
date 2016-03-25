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

def display_5b(hybrid_monthly, hh_count_monthly, monthly_emi, m):
	months = [201401, 201402, 201403, 201404, 201405, 201406, 201407, 201408, 201409, 201410, 201411, 201412]
	hybrid_emi_2014 = []
	hh_2014 = []
	conven_emi_2014 = []
	hybrid_emi_2014.append(hybrid_monthly[10])
	hybrid_emi_2014.append(hybrid_monthly[11])
	hybrid_emi_2014.append(hybrid_monthly[0]+hybrid_monthly[12])
	hybrid_emi_2014.append(hybrid_monthly[1]+hybrid_monthly[13])
	hh_2014.append(hh_count_monthly[10])
	hh_2014.append(hh_count_monthly[11])
	hh_2014.append(hh_count_monthly[0]+hh_count_monthly[12])
	hh_2014.append(hh_count_monthly[1]+hh_count_monthly[13])
	conven_emi_2014.append(monthly_emi[10])
	conven_emi_2014.append(monthly_emi[11])
	conven_emi_2014.append(monthly_emi[12])
	conven_emi_2014.append(monthly_emi[13])	
	for i in range(2, 10):
		hybrid_emi_2014.append(hybrid_monthly[i])
		hh_2014.append(hh_count_monthly[i])
		conven_emi_2014.append(monthly_emi[i])
	count = 0	
	for month in months:
		if month == 201401 or month == 201403 or month == 201405 or month == 201407 or month == 201408 or month == 201410 or month == 201412:
			days = 31
		elif month == 201402:
			days = 28
		else:
			days = 30
		hybrid_emission = hybrid_emi_2014[count]/hh_2014[count]*days*117538000
		print "Reduction in CO2 Emission for %s Electric Miles in MONTH %s: %s TONS" %(m, month, conven_emi_2014[count] - hybrid_emission)
		count+=1

			


print "3a)"
cur.execute("SELECT COUNT(personid) FROM (SELECT personid FROM day_table WHERE trpmiles>=0 GROUP BY houseid, personid) foo;")
total = float(cur.fetchone()[0])
for i in range(1,21):
	queryString = "SELECT COUNT(houseid) FROM (SELECT houseid FROM day_table WHERE trpmiles>=0 GROUP BY houseid, personid HAVING SUM(trpmiles) < %s) foo;" %(5*i)
	cur.execute(queryString)
	trips = float(cur.fetchone()[0])
	print "Percent of Individuals Travelling LESS THAN %s MILES: %f PERCENT" %(5*i, trips/total*100)

print ""
print "3b)"
for i in range(1,21):
	queryString = "SELECT AVG(epatmpg) FROM day_table v INNER JOIN veh_table d ON d.houseid=v.houseid AND \
		v.vehid=d.vehid AND v.vehid>=1 WHERE trpmiles < %s AND trpmiles > 0 AND drvr_flg=1;" %(5*i)
	cur.execute(queryString)
	avg_mpg = float(cur.fetchone()[0])
	print "Average Fuel Economy of Trips LESS THAN %s MILES: %f MPG" %(5*i, avg_mpg)

print ""
print "3c)"
monthly_emi = []
months = [200803, 200804, 200805, 200806, 200807, 200808, 200809, 200810, 200811, 200812, 200901, 200902, 200903, 200904]
for month in months:
	if month == 200803 or month == 200805 or month == 200807 or month == 200808 or month == 200810 or month == 200812 or month == 200901 or month == 200903:
		days = 31
	elif month == 200902:
		days = 28
	else:
		days = 30
	queryString = "SELECT COUNT(DISTINCT houseid) FROM day_table WHERE tdaydate=%s AND trpmiles > 0 AND drvr_flg=1;" %(month)
	cur.execute(queryString)
	hh_count = float(cur.fetchone()[0])	
	queryString = "SELECT SUM(trpmiles/epatmpg) FROM veh_table JOIN day_table USING \
		(houseid, vehid) WHERE tdaydate=%s AND trpmiles > 0 AND drvr_flg=1;" %(month)
	cur.execute(queryString)
	hh_emission = (float(cur.fetchone()[0])*0.008887)/hh_count*117538000*days
	monthly_emi.append(hh_emission)
	queryString = "SELECT value*1000000 FROM transport_data WHERE col=12 AND date=%s;" %(month)
	cur.execute(queryString)
	total_emission =  float(cur.fetchone()[0])
	print "Percent of Tranportation Emissions by Households in MONTH %s: %f PERCENT" %(month, hh_emission/total_emission*100)	
conven_emission = sum(monthly_emi)

print ""
print "3d)"
ranges = [20, 40, 60]
for m in ranges:
	savings = []
	count = 0
	for month in months:
		if month == 200803 or month == 200805 or month == 200807 or month == 200808 or month == 200810 or month == 200812 or month == 200901 or month == 200903:
			days = 31
		elif month == 200902:
			days = 28
		else:
			days = 30	
			
		#gives hh_count for that survey month	
		queryString = "SELECT COUNT(DISTINCT houseid) FROM day_table WHERE tdaydate=%s AND trpmiles > 0 AND drvr_flg=1;" %(month)
		cur.execute(queryString)
		hh_count = float(cur.fetchone()[0])		

		#gives total CO2 emmision for hybrid
		queryString = "SELECT e.value/m.value FROM electric_data e, mkwh_data m WHERE e.col=9 AND m.col=13 \
			AND e.date=%s AND m.date=%s;" %(month, month) #CO2/kwh ratio for the month
		cur.execute(queryString)
		ratio = float(cur.fetchone()[0])	
		queryString = "SELECT SUM(s/(mpg*0.0906)) FROM (SELECT epatmpg AS mpg, SUM(trpmiles) AS s FROM veh_table v JOIN day_table USING \
			(houseid, vehid) WHERE trpmiles>0 AND tdaydate=%s AND drvr_flg=1 \
			GROUP BY v.houseid, v.vehid, epatmpg HAVING SUM(trpmiles) <= %s) foo;" %(month, m) #total kwh for vehicles w/ total miles <= 20
		cur.execute(queryString)
		kwh = float(cur.fetchone()[0])
		emi_under_m = kwh*ratio #total CO2 emmission for vehicles w/ total miles <= m
		queryString = "SELECT SUM(%s/(mpg*0.0906)) FROM (SELECT epatmpg AS mpg FROM veh_table v JOIN day_table USING \
			(houseid, vehid) WHERE trpmiles>0 AND tdaydate=%s AND drvr_flg=1 \
			GROUP BY v.houseid, v.vehid, epatmpg HAVING SUM(trpmiles) > %s) foo;" %(m, month, m) #total kwh of first m miles for vehicles w/ total miles > m
		cur.execute(queryString)
		kwh = float(cur.fetchone()[0])
		emi_first_m = kwh*ratio #total CO2 emmission for first m kwh miles for vehicles w/ total miles > m
		queryString = "SELECT SUM((s-%s)/mpg)*0.008887 FROM (SELECT epatmpg AS mpg, SUM(trpmiles) AS s FROM veh_table v JOIN day_table USING \
			(houseid, vehid) WHERE trpmiles>0 AND tdaydate=%s AND drvr_flg=1 \
			GROUP BY v.houseid, v.vehid, epatmpg HAVING SUM(trpmiles) > %s) foo;" %(m, month, m)
		cur.execute(queryString)
		emi_rest = float(cur.fetchone()[0]) #total of the rest of CO2 emissions
		hybrid_emission = (emi_under_m+emi_first_m+emi_rest)/hh_count*days*117538000
		savings.append(hybrid_emission)
		print "Reduction in CO2 Emission For %s Electric Miles in MONTH %s: %s TONS" %(m, month, monthly_emi[count] - hybrid_emission)
		count +=1
	print "Reudction in CO2 Emission Over Surveyed Months For %s Electric Miles: %s TONS" %(m, conven_emission-sum(savings))
	print ""


print "5a)"
ranges = [84, 107, 208, 270]
for m in ranges:
	savings = []
	count = 0
	for month in months:
		if month == 200803 or month == 200805 or month == 200807 or month == 200808 or month == 200810 or month == 200812 or month == 200901 or month == 200903:
			days = 31
		elif month == 200902:
			days = 28
		else:
			days = 30	
			
		#gives hh_count for that survey month	
		queryString = "SELECT COUNT(DISTINCT houseid) FROM day_table WHERE tdaydate=%s AND trpmiles > 0 AND drvr_flg=1;" %(month)
		cur.execute(queryString)
		hh_count = float(cur.fetchone()[0])		

		#gives total CO2 emmision for hybrid
		queryString = "SELECT e.value/m.value FROM electric_data e, mkwh_data m WHERE e.col=9 AND m.col=13 \
			AND e.date=%s AND m.date=%s;" %(month, month) #CO2/kwh ratio for the month
		cur.execute(queryString)
		ratio = float(cur.fetchone()[0])
		#print ratio
		queryString = "SELECT SUM(s/(mpg*0.0906)) FROM (SELECT epatmpg AS mpg, SUM(trpmiles) AS s FROM veh_table v JOIN day_table USING \
			(houseid, vehid) WHERE trpmiles>0 AND tdaydate=%s AND drvr_flg=1 \
			GROUP BY v.houseid, v.vehid, epatmpg HAVING SUM(trpmiles) <= %s) foo;" %(month, m) #total kwh for vehicles w/ total miles <= m
		cur.execute(queryString)
		kwh = float(cur.fetchone()[0])
		#print kwh
		emi_under_m = kwh*ratio #total CO2 emmission for vehicles w/ total miles <= m
		queryString = "SELECT SUM(s/mpg)*0.008887 FROM (SELECT epatmpg AS mpg, SUM(trpmiles) AS s FROM veh_table v JOIN day_table USING \
			(houseid, vehid) WHERE trpmiles>0 AND tdaydate=%s AND drvr_flg=1 \
			GROUP BY v.houseid, v.vehid, epatmpg HAVING SUM(trpmiles) > %s) foo;" %(month, m)
		cur.execute(queryString)
		emi_over_m = float(cur.fetchone()[0]) #total of the rest of CO2 emissions
		hybrid_emission = (emi_under_m+emi_over_m)/hh_count*days*117538000
		savings.append(hybrid_emission)
		print "Reduction in CO2 Emission for %s Electric Miles in MONTH %s: %s TONS" %(m, month, monthly_emi[count] - hybrid_emission)
		count +=1	
	print "Reduction in CO2 Emission Over Surveyed Months for %s Electric Miles: %s TONS" %(m, conven_emission-sum(savings))	
	print ""


print "5b)"
ranges = [84, 107, 208, 270]
m_2014 = [201403, 201404, 201405, 201406, 201407, 201408, 201409, 201410, 201411, 201412, 201401, 201402, 201403, 201404]
for m in ranges:
	hybrid_monthly = []
	hh_count_monthly = []
	count = 0
	for month in months:
		if month == 200803 or month == 200805 or month == 200807 or month == 200808 or month == 200810 or month == 200812 or month == 200901 or month == 200903:
			days = 31
		elif month == 200902:
			days = 28
		else:
			days = 30	
			
		#gives hh_count for that survey month	
		queryString = "SELECT COUNT(DISTINCT houseid) FROM day_table WHERE tdaydate=%s AND trpmiles > 0;" %(month)
		cur.execute(queryString)
		hh_count = float(cur.fetchone()[0])		
		hh_count_monthly.append(hh_count)

		#gives total CO2 emmision for hybrid
		queryString = "SELECT SUM(value) FROM((SELECT value FROM mkwh_data WHERE col = 3 AND date = %s) \
			UNION (SELECT value FROM mkwh_data WHERE col = 5 AND date = %s) \
			UNION (SELECT value FROM mkwh_data WHERE col = 12 AND date = %s)) f;" %(m_2014[count], m_2014[count], m_2014[count])
		cur.execute(queryString)
		total_mkwh = float(cur.fetchone()[0])	#total mkwh from NG, wind, nuclear
		queryString = "SELECT value FROM electric_data WHERE col = 2 AND date = %s;" %(m_2014[count])
		cur.execute(queryString)
		ratio = float(cur.fetchone()[0])/total_mkwh		#CO2/mkwh ratio; only NG emits CO2 
		queryString = "SELECT SUM(s/(mpg*0.0906)) FROM (SELECT epatmpg AS mpg, SUM(trpmiles) AS s FROM veh_table v INNER JOIN day_table d ON \
			v.houseid=d.houseid AND v.vehid=d.vehid AND trpmiles>0 AND tdaydate=%s AND drvr_flg=1 \
			GROUP BY v.houseid, v.vehid, epatmpg HAVING SUM(trpmiles) <= %s) foo;" %(month, m) #total kwh for vehicles w/ total miles <= m
		cur.execute(queryString)
		kwh = float(cur.fetchone()[0])
		emi_under_m = kwh*ratio #total CO2 emmission for vehicles w/ total miles <= m
		queryString = "SELECT SUM(s/mpg)*0.008887 FROM (SELECT epatmpg AS mpg, SUM(trpmiles) AS s FROM veh_table v INNER JOIN day_table d ON \
			v.houseid=d.houseid AND v.vehid=d.vehid AND trpmiles>0 AND tdaydate=%s AND drvr_flg=1 \
			GROUP BY v.houseid, v.vehid, epatmpg HAVING SUM(trpmiles) > %s) foo;" %(month, m)
		cur.execute(queryString)
		emi_over_m = float(cur.fetchone()[0]) #total of the rest of CO2 emissions
		hybrid_emission = (emi_under_m+emi_over_m)
		#temp = hybrid_emission/hh_count*days*117538000
		hybrid_monthly.append(hybrid_emission)
		#print "Change in CO2 Emission for %s Electric Miles in MONTH %s: %s TONS" %(m, month, monthly_emi[count] - temp)
		count +=1
	display_5b(hybrid_monthly, hh_count_monthly, monthly_emi, m)
	print ""
		#print "Change in CO2 Emission for %s Electric Miles in MONTH %s: %s TONS" %(m, month, monthly_emi[month_count] - hybrid_emission)
	#print "Total Change in CO2 Emission Over Surveyed Months For %s Electric Miles: %s" %(m, conven_emission-sum(savings))		

con.commit()
