import csv

with open("./all_sites.csv","rU") as source:
    rdr= csv.reader( source )
    with open("/Users/Flo/David/Cours/HDP1/out.csv","wb") as result:
        wtr= csv.writer( result )
        for r in rdr:
            wtr.writerow( (r[0], r[1], r[2], r[3]) )