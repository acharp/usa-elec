import csv
import datetime
import time

with open('./weatherdata.csv', 'rU') as weatherdata_csv:
	weather_reader = csv.reader(weatherdata_csv, delimiter=',')

	for weather in weather_reader:
		site_id = weather[0]
		timestamp = weather[1]
		temperature = weather[2]
		time_trunc = datetime.datetime.fromtimestamp(float(timestamp)).strftime('%Y-%m-%d %H') 

		with open('./csv-only/csv/' + site_id + ".csv", 'rU') as elecdata_csv:
			elec_reader = csv.reader(elecdata_csv, delimiter=',')

			for elec in elec_reader:
				if time_trunc in elec[1]:
					with open('./consumptiontable.csv', 'ab') as result_csv:
						csv_writer = csv.writer(result_csv, delimiter=',')
						#print "%s, %s, %s, %s" % (site_id, int(time.mktime(datetime.datetime.strptime(elec[1], '%Y-%m-%d %H:%M:%S').timetuple())), temperature, elec[2])
						csv_writer.writerow([site_id, int(time.mktime(datetime.datetime.strptime(elec[1], '%Y-%m-%d %H:%M:%S').timetuple())), temperature, elec[2]])
