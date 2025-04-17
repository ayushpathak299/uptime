import requests
#import time
import json
#import urllib.request
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from datetime import date

from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

class OutageData:
    df = pd.read_csv('monitiorgroup.csv')

    url = "https://accounts.zoho.com/oauth/v2/token"
    client_id = os.getenv("ZOHO_CLIENT_ID")
    client_secret = os.getenv("ZOHO_CLIENT_SECRET")
    refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")

    payload = f"client_id={client_id}&client_secret={client_secret}&refresh_token={refresh_token}&grant_type=refresh_token"

    payload = "client_id=1000.F3ECHYKUK9ASR29PZ3RRKU5H8EE9UJ&client_secret=583fc4a3dd3aed419a479395ad32c0fb168632af94&refresh_token=1000.24a7e879923148a3c8c758c890a4d646.58bcfb73c2395339b0e0a3100de8de1a&grant_type=refresh_token"
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache"
    }
    response = requests.request("POST", url, data=payload, headers=headers)
    r = response.json()
    myToken = r.get('access_token')
    print(myToken)

# date and time
    # date and time
    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    yesterday = date.today() - timedelta(1)
    start_date = yesterday
    end_date = yesterday + timedelta(1)#date(datetime.today().year, datetime.today().month, datetime.today().day)
    for single_date in daterange(start_date, end_date):
        ldate = single_date.strftime("%Y-%m-%d")
        start_date = ldate

    #startdate =  date(2022, 1, 1) #date.today() - timedelta(1)
    #enddate = date.today() - timedelta(1)
    #start_date = enddate
        res =  list(df.monitor_group_id)
        res1 = list(df.monitor_group_product)
        res2 = list(df.monitor_group_region)
        for i in range(len(res)):
            grpid = res[i]
            grppdt = res1[i]
            grprgn = res2[i]
            url = "https://www.site24x7.com/api/reports/summary/group/{}?period=50&start_date={}&end_date={}".format(grpid,start_date,ldate)
            #payload = "period=50&start_date="startdate"&end_date="enddate"
            headers = {
                'Authorization' : "Bearer " +myToken ,
                'Content-Type': "application/json",
                'cache-control': "no-cache",
                'Accept-Charset' : "UTF-8",
                'Version' : "2.0",
                  }
            print (url)
            print(start_date)
            print(ldate)
            responseobj = requests.request("GET", url, headers=headers)
            #print (response.json())
            dataobj = json.loads(responseobj.content.decode('utf-8'))
            data = dataobj["data"]
            outage_details = data["outage_details"]
            summary_details = data["summary_details"]
            availability_details = len(data['availability_details'])


            fdate = data["info"]
            insertqueries=[]
            templist = ()


            connection = psycopg2.connect(
                host="jira-redash.c5ditj8vhg0k.us-west-1.rds.amazonaws.com",
                database="jira",
                user="redash",
                password="N6ZrFz8KdR",
                port = "5432"
                )
            connection.autocommit = True
            cursor = connection.cursor()
            sqlquery2 = """ INSERT INTO mguptimesummary_neu_copy(maintenance_percentage, alarm_count, availability_duration,unmanaged_percentage,unmanaged_duration,downtime_duration,downtime_percentage,down_count,availability_percentage,maintenance_duration,mttr,mtbf,mgid,summarydate,mgpr,mgrg)
                                                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
            maintenance_percentage = float(summary_details['maintenance_percentage'])
            alarm_count = int(summary_details['alarm_count'])
            availability_duration = summary_details['availability_duration']
            unmanaged_percentage = float(summary_details['unmanaged_percentage'])
            unmanaged_duration = summary_details['unmanaged_duration']
            downtime_duration = summary_details['downtime_duration']
            downtime_percentage = float(summary_details['downtime_percentage'])
            down_count = int(summary_details['down_count'])
            availability_percentage = float(summary_details['availability_percentage'])
            maintenance_duration = summary_details['maintenance_duration']
            mttr = summary_details['mttr']
            mtbf = summary_details['mtbf']
            mgid = int(grpid)
            summarydate = fdate['start_time']
            grpepdt = grppdt
            grpergn = grprgn

            templist = ()
            insertqueries = []
            templist = (maintenance_percentage,alarm_count,availability_duration,unmanaged_percentage,unmanaged_duration,downtime_duration,downtime_percentage,down_count,availability_percentage,maintenance_duration,mttr,mtbf,mgid,summarydate,grpepdt, grpergn)
            insertqueries.append(templist)
            result = cursor.executemany(sqlquery2, insertqueries)
            connection.commit()
            print(cursor.rowcount, "Record inserted successfully into mguptimesummary_neu table")
            connection.close()