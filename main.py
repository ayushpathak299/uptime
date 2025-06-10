import requests
#import time
import json
#import urllib.request
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from datetime import date
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

# Load environment variables
load_dotenv()


def send_failure_slack(error_message):
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not slack_webhook_url:
        print("Slack webhook URL is missing. Cannot send failure notification.")
        return

    payload = {
        "text": f"🚨 *ETL Job Alert - Site24x7* 🚨\n\n*Error Details:* {error_message}\n\n*Please check immediately.*"
    }

    try:
        response = requests.post(slack_webhook_url, json=payload)
        if response.status_code == 200:
            print("Slack notification sent successfully!")
        else:
            print(f"Failed to send Slack notification: {response.text}")
    except Exception as e:
        print(f"Exception while sending Slack notification: {e}")

class OutageData:
    df = pd.read_csv('monitiorgroup.csv')
    url = "https://accounts.zoho.com/oauth/v2/token"
    client_id = os.getenv("ZOHO_CLIENT_ID")
    client_secret = os.getenv("ZOHO_CLIENT_SECRET")
    refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")

    payload = f"client_id={client_id}&client_secret={client_secret}&refresh_token={refresh_token}&grant_type=refresh_token"

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
    end_date = yesterday + timedelta(1)
    #date(datetime.today().year, datetime.today().month, datetime.today().day)

    # start_date = date(2025, 5, 10)  # Start from 2025-01-01
    # yesterday = date(2025, 5, 11)   # End at yesterday
    # end_date = yesterday
    total_records_inserted = 0
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
            try:
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
                    host=os.getenv("DB_HOST"),
                    database=os.getenv("DB_NAME"),
                    user=os.getenv("DB_USER"),
                    password=os.getenv("DB_PASSWORD"),
                    port = os.getenv("DB_PORT")
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
                total_records_inserted += 1
            except Exception as e:
                print(f"ETL failed: {e}")
                send_failure_slack(str(e))

            finally:
                if connection:
                    connection.close()
    try:
        connection = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        connection.autocommit = True
        cursor = connection.cursor()

        source = 'site24X7'
        run_date = start_date

        sqlquery3 = """INSERT INTO etl_status_log (source, record_count, run_date) VALUES (%s, %s, %s)"""
        cursor.execute(sqlquery3, (source, total_records_inserted, run_date))
        connection.commit()
        print(f"ETL status logged successfully with {total_records_inserted} total records.")
        if total_records_inserted<35 :
            send_failure_slack("records inserted less than expected")
        if total_records_inserted>35 :
            send_failure_slack("duplicate records are inserted")


    except Exception as e:
        print(f"Failed to log ETL summary: {e}")
        send_failure_slack(str(e))

    finally:
        if connection:
            connection.close()

