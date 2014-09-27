from ftplib import FTP
from subprocess import call
import csv
import Socrata
from datetime import datetime, timedelta
import time
import redis
import os

ftp = FTP('cchftp1.honolulu.gov')   # connect to host, default port
ftp.login(os.environ['CCHFTPUSER'], os.environ['CCHFTPPASS'])

ftp.retrbinary('RETR HPDTrafficExtract.txt', open('HPDTrafficExtract.txt', 'wb').write)

redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379/')

r = redis.from_url(redis_url)

last_traffic = r.get('lasttraffic')
print last_traffic
if(last_traffic == None):
    last_traffic = 0

last_traffic_date = datetime.fromtimestamp(int(last_traffic))


creader = csv.reader(open('HPDTrafficExtract.txt', 'rb'), delimiter='|')

#A list of the traffic incident names

incident_codes = {"633": "STALLED/HAZARDOUS VEHICLE",
                  "550": "MOTOR VEHICLE COLLISION",
                  "630": "TRAFFIC NUISANCE OR PARKING VIOLATION",
                  "560": "TRAFFIC INCIDENT - NO COLLISION",
                  "632": "HAZARDOUS DRIVER",
                  "550V":"MOTOR VEHICLE COLLISION - TOWED",
                  "634": "TRAFFIC SIGNAL PROBLEM"
                  }

def create_dataset_with_columns(dataset, title = 'Traffic Insidents', description = ''):
    """Creates a new Socrata dataset with columns for an RSS feed"""
    try:
        dataset.create(title, description)
    except Socrata.DuplicateDatasetError:
        print "This dataset already exists."
        return False

    dataset.add_column('Date', '', 'date')
    dataset.add_column('Code', '', 'text', False, False, 300)
    dataset.add_column('Type', '', 'text', False, False, 300)
    dataset.add_column('Address', '', 'text', False, False, 300)
    dataset.add_column('Location', '', 'text', False, False, 300)
    dataset.add_column('Area', '', 'text', False, False, 300)
    return

host      = "https://data.honolulu.gov"
username  = os.getenv("SOCRATA_USER")
password  = os.getenv("SOCRATA_PASS")
app_token = os.getenv("SOCRATA_APP_TOKEN")

dataset = Socrata.Dataset(host, username, password, app_token)

if(os.getenv("TI_DATASET_ID", "") != ""):
    dataset.use_existing(os.getenv("TI_DATASET_ID"))
else:
    create_dataset_with_columns(dataset)

if  dataset:
    batch_requests = []
    cutoff = last_traffic_date
    count = 0
    for row in creader:
        data          = {}
        rowtime = datetime.strptime(row[0], "%m/%d/%Y %I:%M:%S %p")
        data['Date'] = rowtime.strftime("%m/%d/%Y %H:%M:%S")
        data["Code"] = row[1]
        data["Type"] = row[1]
        if(row[1] in incident_codes):
            data["Type"] = incident_codes[row[1]]
        data["Address"] = row[2]
        data["Location"] = row[3]
        data["Area"] = row[4]

        if(rowtime > last_traffic_date):
            last_traffic_date = rowtime
        if(rowtime > cutoff):
            batch_requests.append(dataset.add_row_delayed(data))
        count+=1

        if(len(batch_requests) > 100):
            print batch_requests
            dataset._batch(batch_requests)
            batch_requests=[]
            print count

    if(len(batch_requests) > 0):
        dataset._batch(batch_requests)
    print "You can now view the dataset:"
    print dataset.short_url()
else:
    print "There was an error creating your dataset."

print "\nFinished"

r.set("lasttraffic", str(int(time.mktime(last_traffic_date.timetuple()))))
