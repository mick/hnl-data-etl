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

ftp.retrbinary('RETR hpdcm.zip', open('hpdcm.zip', 'wb').write)
return_code = call("./unzip -o -P "+os.environ['ZIPPASS']+" hpdcm.zip", shell=True)

current_date = datetime.now()
month_ahead_date = datetime.now() + timedelta(days=30)
month_ago_date = datetime.now() -timedelta(days=30)

redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379/')
r = redis.from_url(redis_url)
seen_ids = list(r.smembers("crimeincidents-"+str(current_date.month)))

seen_ids.extend(list(r.smembers("crimeincidents-"+str(month_ago_date.month))))

r.expireat("crimeincidents-"+str(current_date.month), int(time.mktime(month_ahead_date.timetuple())))

new_seen_ids = []
# compare id and elminate dupes
creader = csv.reader(open('cm_incidents.csv', 'rb'))


def create_dataset_with_columns(dataset, title = 'Crime Insidents', description = ''):
    """Creates a new Socrata dataset with columns for an RSS feed"""
    try:
        dataset.create(title, description)
    except Socrata.DuplicateDatasetError:
        print "This dataset already exists."
        return False

# "ObjectID","KILO_NBR","BLOCK_ADD","CM_ID","CM_AGENCY","CVDATE","CVTIME","CM_LEGEND","Status","Score","Side","Shape"
# 35,"LHP120714000919","1800 BLOCK ALA MOANA","Honolulu_PD_HI_LHP120714000919_060","Honolulu PD, HI",20120714,"1318","THEFT/LARCENY","U",0,"",

    dataset.add_column('ObjectID', '', 'text', False, False, 300)
    dataset.add_column('KiloNBR', '', 'text', False, False, 300)
    dataset.add_column('BlockAddress', '', 'text', False, False, 300)
    dataset.add_column('CMID', '', 'text', False, False, 300)
    dataset.add_column('CMAgency', '', 'text', False, False, 300)
    dataset.add_column('Date', '', 'date')
    dataset.add_column('Type', '', 'text', False, False, 300)
    dataset.add_column('Status', '', 'text', False, False, 300)
    dataset.add_column('Score', '', 'text', False, False, 300)
    dataset.add_column('Side', '', 'text', False, False, 300)


    return


host      = "https://data.honolulu.gov"
username  = os.getenv("SOCRATA_USER")
password  = os.getenv("SOCRATA_PASS")
app_token = os.getenv("SOCRATA_APP_TOKEN")

dataset = Socrata.Dataset(host, username, password, app_token)


# "g6yu-aty9"

if(os.getenv("CM_DATASET_ID", "") != ""):
    dataset.use_existing(os.getenv("CM_DATASET_ID"))
else:
    create_dataset_with_columns(dataset)

if  dataset:
    batch_requests = []


    cutoff = datetime.now() - timedelta(days=7)

    count = 0
    for row in creader:
        if(count > 0):

            data          = {}
            data["ObjectID"] = row[0]
            data["KiloNBR"] = row[1]
            data["BlockAddress"] = row[2]
            data["CMID"] = row[3]
            data["CMAgency"] = row[4]
            rowtime = datetime(int(row[5][:4]), int(row[5][4:6]), int(row[5][6:8]), int(row[6][:2]), int(row[6][2:4]))
            data['Date'] = rowtime.strftime("%m/%d/%Y %H:%M:%S")
            data["Type"]   = row[7]
            data["Status"] = row[8]
            data["Score"] = row[9]
            data["Side"] = row[10]

            if(rowtime > cutoff and data["KiloNBR"] not in seen_ids):
                batch_requests.append(dataset.add_row_delayed(data))
                new_seen_ids.append(data["KiloNBR"])
        count+=1

        if(len(batch_requests) >49):
            #print batch_requests
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


# add ids as we post rows to the api.

#save these to redis with a 2 week expire
#print new_seen_ids

for sid in new_seen_ids:
    r.sadd("crimeincidents-"+str(current_date.month), sid)
