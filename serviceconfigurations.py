# import statements
import os
import random
import sys
from datetime import datetime, time as dt_time
from pathlib import Path
from threading import Thread
import threading
import mysql.connector
import snowflake.connector
from snowflake.connector import DictCursor
import concurrent.futures
import pytz
import requests
import urllib
import queue
from queue import Queue
import logging
import re
from datetime import datetime, timedelta
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import subprocess
import traceback
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from sqlalchemy.engine import URL
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# PATH CONFIGS

SCRIPT_PATH = "/gmservices/QA_Services/FEED_NON_OPENERS_SHARING/"
LOG_PATH = "/gmservices/QA_Services/FEED_NON_OPENERS_SHARING/logs/"
FILE_PATH = ""
PID_FILE = ""

THREAD_COUNT = 1  # thread count

# mail configs
FROM_EMAIL = "noreply@alerts.zetaglobal.net"
RECEPIENT_EMAILS = []
MAIL_FILE = ''

# skype configs
skype_configurations = {
    'url': 'http://zds-prod-ext-greenapp1-vip.bo3.e-dialog.com/sendSkypeAlerts/index.php?key=',
    'file_path': SCRIPT_PATH,
    'default_channel': '19:69777cb1e0d94ef9ba894c5d4d7eb3b6@thread.skype',
    'script_path': SCRIPT_PATH,
    'script_name': 'Non Openers',
    'server': 'zdl3-mn09.bo3.e-dialog.com',
    'log_path': LOG_PATH
}

# Mysql Configurations
MYSQL_CONNECTION_RETRY_LIMIT = 5

MDB_MYSQL_CONFIGS = {
    'drivername': 'mysql+pymysql',
    'username': 'techuser',
    'password': 'tech12#$',
    'host': 'zds-prod-mdb-vip.bo3.e-dialog.com',
    'database': 'PFM_UNIVERSAL_DB_QA',
}
JBDB4_MYSQL_CONFIGS = {
    'drivername': 'mysql+pymysql',
    'username': 'techuser',
    'password': 'tech12#$',
    'host': 'zds-prod-jbdb4-vip.bo3.e-dialog.com',
    'database': 'RT_CUSTOMIZATION_DB',
}

# TABLE NAME CONFIGS
POST_PROCESSING_TABLES = [
    "RT_NONOPENER_FEED_LEVEL_SHARING_TRANSACTIONAL_DATA",
    "RT_NONOPENER_FEED_LEVEL_SHARING_TRANSACTIONAL_DATA_OTEAM"
]
GREEN_OPEN_TABLE = 'CUST_REPORT_DB.APT_OPEN_DETAILS'
INFS_OPEN_TABLE = 'CUST_REPORT_DB.APT_OPEN_DETAILS_OTEAM'
DELIVERED_TABLE = 'PMTA_RAW_DELIVERED_LOG'
QUOTA_CHECK_TABLE = 'RT_NONOPENER_SHARING_QUOTA_CHECK'
TARGET_DETAILS_TABLE = 'FEED_NONOPENER_SHARING_TARGET'
# MYSQL QUERIES
FETCH_WAITING_RECORDS_QUERY = "SELECT * FROM {table_name} WHERE  NO_STATUS = 'W' AND createdDate <= NOW()"
CHECK_FOR_RESPONDER_QUERY = "SELECT count(1) FROM  {table_name} WHERE SUBID = '{subid}' and PROFILEID = '{profileid}' and LASTOPENDATE >= NOW() - INTERVAL 1 DAY"
CHECK_FOR_DELIVERED_QUERY = "SELECT count(1) FROM RT_CUSTOMIZATION_DB.PMTA_RAW_DELIVERED_LOG where SUBID ='{subid}' AND TOADDRESS = '{email}' and TIMELOGGED >=NOW() - INTERVAL 1 DAY"
UPDATE_POST_PROCESSING_TABLE_STATUS_QUERY = "UPDATE {table_name} set NO_STATUS = '{status}' where SUBID = '{subid}' and profileId = {profileid}"
CHECK_GREEN_FEED_SUPP_EMAIL_LEVEL_QUERY = "SELECT COUNT(1) FROM {table_name} where email = '{email}'"
CHECK_GREEN_FEED_SUPP_EMAIL_LISTID_LEVEL_QUERY = "SELECT COUNT(1) FROM ({query}) G where email  ='{email}'  and G.listid = '{listid}'"

CHECK_INFS_FEED_SUPP_EMAIL_LEVEL_QUERY = "SELECT COUNT(1) FROM {table_name} where email = '{email}'"
CHECK_INFS_FEED_SUPP_EMAIL_LISTID_LEVEL_QUERY = "SELECT COUNT(1) FROM ({query}) G where email  ='{email}'  and G.listid = '{listid}'"

GET_TARGET_LISTID_INFO_QUERY = "SELECT {listidcolumn} FROM FEED_NONOPENER_SHARING_TARGET WHERE targetId = {targetid}  and channelName = '{channel}' limit 1"

GET_TRANSACTIONAL_TABLE_INFO_QUERY = "SELECT DISTINCT transactionalTable FROM FEED_NONOPENER_SHARING_SOURCE WHERE {listidcolumn} = '{listid}'  and channelName = '{channel}' limit 1"
FETCH_DATA_FROM_TRANSACTIONAL_QUERY = "SELECT email,fname, lname, zipcode, city ,address, state , url, listid, ipaddress,signupdate,vertical, dob , subid  from {table_name}  where id = {transactionalId} "

FETCH_TARGET_QUOTA_DETAILS = '''SELECT t.targetId, t.quota, t.quotaType, COALESCE(q.count, 0) AS currentCount
        FROM FEED_NONOPENER_SHARING_TARGET t
        LEFT JOIN RT_NONOPENER_SHARING_QUOTA_CHECK q
            ON t.targetId = q.targetId
            AND ((t.quotaType = 'H' AND q.hour = HOUR(NOW()) AND q.deployedDate = CURDATE())
                 OR (t.quotaType = 'D' AND q.deployedDate = CURDATE()))
        WHERE t.sourceId = {sourceid}'''
UPDATE_POST_PROCESSING_TABLE_TARGET_LISTID_QUERY = "UPDATE {table_name} set targetListId = {targetListId} where id = {id}"

# Feed Level Suppressions configs
GREEN_FEED_LEVEL_SUPP_TABLES = {
    'email': (
        "PFM_UNIVERSAL_DB.APT_CUSTDOD_GLOBAL_COMPLAINER_EMAILS",
        "CUST_REPORT_DB.APT_ABUSE_DETAILS",
        "PFM_UNIVERSAL_DB.PFM_FLUENT_REGISTRATIONS_CANADA",
        "PFM_UNIVERSAL_DB.APT_CUSTOM_GLOBAL_HARDBOUNCES_DATA",
        "PFM_UNIVERSAL_DB.APT_CUSTOM_GLOBAL_SOFTINACTIVE",
    ),
    'email_listid': (
        "select email,listid from CUST_REPORT_DB.APT_UNSUB_DETAILS where listid in (select distinct listid from  PFM_UNIVERSAL_DB.PFM_FLUENT_REGISTRATIONS_LOOKUP_DONOTDROP_RT where RULE in (2,3) ) ",
    )
}

INFS_FEED_LEVEL_SUPP_TABLES = {
    'email': (
        "INFS_DB.APT_ADHOC_GLOBAL_SUPP_20210204",
        "CUST_REPORT_DB.APT_ABUSE_DETAILS",
        "PFM_UNIVERSAL_DB.APT_CUSTOM_GLOBAL_HARDBOUNCES_DATA",
        "INFS_DB.INFS_HARDS",
        "PFM_UNIVERSAL_DB.APT_CUSTOM_GLOBAL_SOFTINACTIVE",
        "INFS_DB.INFS_SOFTS",
    ),
    'email_listid': (
        "select email, listid from CUST_REPORT_DB.APT_UNSUB_DETAILS_OTEAM",
        "select email,listid from CUST_REPORT_DB.APT_EMAIL_REPLIES_TRANSACTIONAL a join INFS_DB.INFS_ADHOC_DOMAINS b on lower(trim(a.domain))=lower(trim(b.domain)) where a.id > 17218326",
        "select email,listid from INFS_DB.INFS_UNSUBS_ACCOUNT_WISE a join (select distinct listid, acc_name as account_name from INFS_DB.INFS_ADHOC_DOMAINS) b on a.account_name=b.account_name",
        "select email, listid from INFS_DB.APT_INFS_ACCOUNT_LEVEL_STATIC_SUPPRESSION_DATA",
    )
}


