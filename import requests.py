import requests

url = 'https://opendata.ecdc.europa.eu/covid19/nationalcasedeath_eueea_daily_ei/csv/data.csv'
r = requests.get(url, allow_redirects=True)

open('<fill in your folderpath ending with filename.type>', 'wb').write(r.content)