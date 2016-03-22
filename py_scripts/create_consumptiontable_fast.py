import csv
import datetime
import time
import os

list_csv = os.listdir("./csv-only/csv/")

list_sites = []
for csv_file in list_csv:
	val = csv_file.split(".")[0]
	if val != "":
		list_sites.append(val)

consoDict = dict()
for site_id in list_sites:
	print "Loading site date " + site_id + "..." 
	with open('./csv-only/csv/' + site_id + ".csv", 'rU') as elecdata_csv:
		elec_reader = csv.reader(elecdata_csv, delimiter=',')
			
		for elec in elec_reader:
			elecDict = dict((int(rows[0]),rows[2]) for rows in elec_reader)
	
	consoDict[site_id] = elecDict


with open('./weatherdata.csv', 'rU') as weatherdata_csv:
	weather_reader = csv.reader(weatherdata_csv, delimiter=',')

	for weather in weather_reader:
		id = weather[0]
		timestamp = int(weather[1])
		temperature = weather[2]

		for i in range(0, 11):	
			timestamp5min = timestamp + i*300
			with open('./consumptiontable.csv', 'ab') as result_csv:
				if timestamp5min in consoDict[id].keys():
					conso = consoDict[id][timestamp5min]
					csv_writer = csv.writer(result_csv, delimiter=',')
					#print "%s, %s, %s, %s" % (id, timestamp5min, temperature, conso)
					csv_writer.writerow([id, timestamp5min, temperature, conso])
				else:
					print "WARNING: no data for key %s" % (timestamp5min)
