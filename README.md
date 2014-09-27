# HNL Data import

This contains 2 scripts for importing data from the Honolulu city FTP site to
Socrata data portal at data.honolulu.gov

### get_crime.py


Fetches data from ftp site, `ftp://cchftp1.honolulu.gov/hpdcm.zip` Extracts, and
compares to ids kept in redis to avoid posting duplicate entries to Socrata.

This is run daily by Heroku.

The script expects the following ENV Vars:

````
SOCRATA_APP_TOKEN=
SOCRATA_PASS=
SOCRATA_USER=
CCHFTPUSER=
CCHFTPPASS=
ZIPPASS=
CM_DATASET_ID=
````

### get_traffic.py

Fetches data from the ftp site `ftp://cchftp1.honolulu.gov/HPDTrafficExtract.txt`
retrieves the last timestamp from redis and posts anything newer then that to Socrata

This is run every 10mins by Heroku

The script expects the following ENV Vars:

````
SOCRATA_APP_TOKEN=
SOCRATA_PASS=
SOCRATA_USER=
CCHFTPUSER=
CCHFTPPASS=
TI_DATASET_ID=
````

This is run for free on Heroku. The app name is: hnl-data-import. 
