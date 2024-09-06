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
from dateutil.relativedelta import relativedelta
import smtplib
import subprocess
import traceback
import time

from sqlalchemy import URL

from sqlalchemy import create_engine, Column, BigInteger, String, Text, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

SCRIPT_PATH = ""
LOG_PATH = ""
FILE_PATH = ""


THREAD_COUNT = 5  # thread count

TRANSACTIONAL_TABLES_LIST = ["GM_REALTIME_LIVEFEED_TRANSACTIONAL_DATA",
                             "GM_REALTIME_LIVEFEED_TRANSACTIONAL_DATA_OTEAM"
]






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
MDB_MYSQL_CONFIGS = {
    'drivername': 'mysql+pymysql',
    'user': 'techuser',
    'password': 'tech12#$',
    'host': 'zds-prod-mdb-vip.bo3.e-dialog.com',
    'database': 'PFM_UNIVERSAL_DB',
}
