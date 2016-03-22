import forecastio
import time
from datetime import datetime
from datetime import timedelta
import csv
import sys

api_key = sys.argv[1]

def get_temperature_by_hour(date, lat, lng):
    forecast = forecastio.load_forecast(api_key, lat, lng, time=date)
    by_hour = forecast.hourly()
    return by_hour.data

with open('./csv-only/meta/remaining_sites.csv', 'rU') as all_sites_csv:
    sites_reader = csv.reader(all_sites_csv, delimiter=',')
    next(sites_reader, None)  # skip the headers

    for site in sites_reader:
        site_id = site[0]
        latitude = site[4]
        longitude = site[5]
        custom_date = datetime(2011, 12, 31, 0, 0, 0)

        print "Read site data: %s, %s, %s" % (site_id, latitude, longitude)
        print "Writing to csv..."

        with open('./weatherdata_sample.csv', 'ab') as result_csv:
            csv_writer = csv.writer(result_csv, delimiter=',')

            for i in range(0, 367):
                custom_date = custom_date + timedelta(days=1)
                by_hour_data = get_temperature_by_hour(custom_date, latitude, longitude)

                for data in by_hour_data:
                    timestamp = int(time.mktime(data.time.timetuple()))
                    #print "%s, %s (%s), %s" % (site_id, data.time, timestamp, data.temperature)
		    if hasattr(data, 'temperature'):
			temp = data.temperature
			last_temp = temp
		    else:
			temp=last_temp

                    csv_writer.writerow([site_id, timestamp, temp])
