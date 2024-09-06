# import statements


import os
import random
import sys
from datetime import datetime,time as dt_time
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
from sqlalchemy import URL
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

# PATH CONFIGS

SCRIPT_PATH = ""
LOG_PATH = ""
FILE_PATH = ""
PID_FILE = ""

THREAD_COUNT = 5  # thread count



#mail configs
FROM_EMAIL = ""
RECEPIENT_EMAILS = []
MAIL_FILE = ''

#skype configs
skype_configurations = {
    'url': 'http://zds-prod-ext-greenapp1-vip.bo3.e-dialog.com/sendSkypeAlerts/index.php?key=',
    'file_path': SCRIPT_PATH,
    'default_channel': '19:69777cb1e0d94ef9ba894c5d4d7eb3b6@thread.skype',
    'script_path': SCRIPT_PATH,
    'script_name': 'Data Ops',
    'server': 'zdl3-mn09.bo3.e-dialog.com',
    'log_path': LOG_PATH
}



# Mysql Configurations
MYSQL_CONNECTION_RETRY_LIMIT = 5

MDB_MYSQL_CONFIGS = {
    'drivername': 'mysql+pymysql',
    'user': 'techuser',
    'password': 'tech12#$',
    'host': 'zds-prod-mdb-vip.bo3.e-dialog.com',
    'database': 'PFM_UNIVERSAL_DB',
}
JBDB4_MYSQL_CONFIGS = {
    'drivername': 'mysql+pymysql',
    'user': 'techuser',
    'password': 'tech12#$',
    'host': 'zds-prod-jbdb4-vip.bo3.e-dialog.com',
    'database': 'RT_CUSTOMIZATION_DB',
}



#TABLE NAME CONFIGS
POST_PROCESSING_TABLES = [
    "green",
    "orange"
]
GREEN_OPEN_TABLE = 'CUST_REPORT_DB.APT_OPEN_DETAILS'
INFS_OPEN_TABLE = 'CUST_REPORT_DB.APT_OPEN_DETAILS_OTEAM'
DELIVERED_TABLE = 'PMTA_RAW_DELIVERED_LOG'

# MYSQL QUERIES
FETCH_WAITING_RECORDS_QUERY = "SELECT * FROM {table_name} WHERE  NO_STATUS = 'W' AND SEND_AT <= NOW()"
CHECK_FOR_RESPONDER_QUERY = "SELECT count(1) FROM  {table_name} WHERE SUBID = {subid} and PROFILEID = {profileid} and LASTOPENDATE >= NOW() - INTERVAL 1 DAY"
CHECK_FOR_DELIVERED_QUERY = "SELECT count(1) FROM RT_CUSTOMIZATION_DB.PMTA_RAW_DELIVERED_LOG where SUBID ={subid} AND LISTID = {listid} and TIMELOGGED >=NOW() - INTERVAL 1 DAY"
