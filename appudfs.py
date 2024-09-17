from serviceconfigurations import *
from basicudfs import *


def getWaitingRecords(table_name, logger):
    try:
        logger.info(f"Method getWaitingRecords from {table_name} Table invoked...")
        with MySQLSessionManager(logger, MDB_MYSQL_CONFIGS) as mdb_session:
            logger.info(f"Executing query ::: {FETCH_WAITING_RECORDS_QUERY.format(table_name=table_name)}")
            waiting_records = mdb_session.execute(text(FETCH_WAITING_RECORDS_QUERY.format(table_name=table_name)))
            waiting_records_as_dicts = [dict(zip(waiting_records.keys(), row)) for row in waiting_records.fetchall()]
            logger.info(f"Fetched Waiting records for the table  {table_name} ")
            logger.info(f"Sample records are :: {waiting_records_as_dicts[:3]}")
            return waiting_records_as_dicts
    except Exception as e:
        logger.error(f"Unable to Get Waiting requests for the table .. {table_name}")
        logger.error(f"Please look into this..{str(e) + traceback.format_exc()}")
        raise Exception(f"Unable to Get Waiting requests for the table .. {table_name} ED :: {str(e)}")


def isResponderOrNot(request, thread_logger):
    try:
        thread_logger.info(f"Method invoked To check record is responder or not in last 24 hrs...")
        with MySQLSessionManager(thread_logger, MDB_MYSQL_CONFIGS) as mdb_session:
            thread_logger.info(f"Fetching Responder Table info from Source table ")
            thread_logger.info(f"Executing query ::: {FETCH_RESPONDER_INFO_QUERY.format(sourceId=request['sourceId'])}")
            responder_table = mdb_session.execute(text(FETCH_RESPONDER_INFO_QUERY.format(sourceId=request['sourceId'])))
            open_table = responder_table.fetchone()[0]
            thread_logger.info(f"Executing query ::: {CHECK_FOR_RESPONDER_QUERY.format(table_name=open_table, subid=request['subid'],profileid= request['profileId'])}")
            result = mdb_session.execute(text(CHECK_FOR_RESPONDER_QUERY.format(table_name=open_table, subid=request['subid'],profileid= request['profileId'])))
            result = result.fetchone()[0]
            if result >= 1:
                return True
            return False
    except Exception as e:
        thread_logger.info(f"Exception occurred while checking for responder or not::: Please look into this... {request} ::: Error reason :::{str(e) + traceback.format_exc()}")
        raise Exception("Exception occurred while checking for responder or not. ED :: {str(e)}")


def isDeliveredOrNOt(request, thread_logger):
    try:
        thread_logger.info(f"Method invoked to check record is Delivered or not in last 24 hrs...")
        with MySQLSessionManager(thread_logger, JBDB4_MYSQL_CONFIGS) as jbdb_session:
            thread_logger.info(f" Executing query ::: {CHECK_FOR_DELIVERED_QUERY.format(subid=request['subid'], email=request['emailId'])}")
            result = jbdb_session.execute(text(CHECK_FOR_DELIVERED_QUERY.format(subid=request['subid'], email=request['emailId'])))
            result = result.fetchone()[0]
            if result >= 1:
                return True
            return False
    except Exception as e:
        thread_logger.info(f"Exception occurred while checking for Delivered or not::: Please look into this... {request} ::: Error reason :::{str(e) + traceback.format_exc()}")
        raise Exception(f"Exception occurred while checking for Delivered or not.  ED :: {str(e)}")


def isFeedlevelSuppressedOrNot(request, thread_logger):
    thread_logger.info(f"Method invoked to check the record is Feed Level Suppressed or not...")
    channel = request['channel']
    if channel == 'GREEN':
        if checkInGreenFeedSupp(request, thread_logger):
            return True
    elif channel == 'INFS' or channel == 'ORANGE':
        if checkInInfsFeedSupp(request, thread_logger):
            return True
    else:
        thread_logger.info(f"Unknown channel record fetched..Please look into this {request} ")
        return False


def checkInGreenFeedSupp(request, logger):
    try:
        with MySQLSessionManager(logger, MDB_MYSQL_CONFIGS) as mdb_session:
            logger.info(f"Checking in Green Feed Level Suppression's... ")
            logger.info(f" Check for Rule 1 Suppression :: ")
            logger.info(f"Executing query :: {GREEN_CHECK_RULE1_QUERY.format(listid = request['listId'])}")
            result = mdb_session.execute(text(GREEN_CHECK_RULE1_QUERY.format(listid = request['listId'])))
            result = result.fetchone()[0]
            if result >= 1:
                logger.info("This is Rule 1 feed , checking rule 1 suppressed or not ...")
                logger.info(f"Executing Query ::: {GREEN_CHECK_RULE1_SUPPRESSION_QUERY.format(email= request['email'])}")
                result = mdb_session.execute(text(GREEN_CHECK_RULE1_SUPPRESSION_QUERY.format(email=request['email'])))
                result = result.fetchone()[0]
                if result == "C":
                    return True
            else:
                for table in GREEN_FEED_LEVEL_SUPP_TABLES['email']:
                    logger.info(f"Checking the record in Suppressed or not in table {table}")
                    logger.info(f"Executing query ::: {CHECK_GREEN_FEED_SUPP_EMAIL_LEVEL_QUERY.format(table_name=table, email=request['emailId'])}")
                    result = mdb_session.execute(text(CHECK_GREEN_FEED_SUPP_EMAIL_LEVEL_QUERY.format(table_name=table, email=request['emailId'])))
                    result = result.fetchone()[0]
                    if result >= 1:
                        return True
                for table in GREEN_FEED_LEVEL_SUPP_TABLES['email_listid']:
                    logger.info(f"Checking the record in Suppressed or not in table {table}")
                    logger.info(f"Executing query ::: {CHECK_GREEN_FEED_SUPP_EMAIL_LISTID_LEVEL_QUERY.format(query=table, email=request['emailId'],listid =request['listId'])}")
                    result = mdb_session.execute(text(CHECK_GREEN_FEED_SUPP_EMAIL_LISTID_LEVEL_QUERY.format(query=table, email=request['emailId'],listid =request['listId'])))
                    result = result.fetchone()[0]
                    if result >= 1:
                        return True
            return False
    except Exception as e:
        logger.info(f"Exception occurred while checking on Feed level Suppression. Could you please look into this record {request}")
        logger.error(f"Exception details are ::: {str(e) + traceback.format_exc()}")
        raise Exception("Exception occurred while checking on Feed level Suppression ED :: {str(e)}")


def checkInInfsFeedSupp(request, logger):
    try:
        with MySQLSessionManager(logger, MDB_MYSQL_CONFIGS) as mdb_session:
            logger.info(f"Checking in Infs Feed Level Suppression's... ")
            for table in INFS_FEED_LEVEL_SUPP_TABLES['email']:
                logger.info(f"Checking the record in Suppressed or not in table {table}")
                logger.info(f"Executing query ::: {CHECK_INFS_FEED_SUPP_EMAIL_LEVEL_QUERY.format(table_name=table, email=request['emailId'])}")
                result = mdb_session.execute(text(CHECK_INFS_FEED_SUPP_EMAIL_LEVEL_QUERY.format(table_name=table, email=request['emailId'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True
            for table in INFS_FEED_LEVEL_SUPP_TABLES['email_listid']:
                logger.info(f"Checking the record in Suppressed or not in table {table}")
                logger.info(f"Executing query ::: {CHECK_INFS_FEED_SUPP_EMAIL_LISTID_LEVEL_QUERY.format(query=table, email=request['emailId'],listid =request['listId'])}")
                result = mdb_session.execute(text(CHECK_INFS_FEED_SUPP_EMAIL_LISTID_LEVEL_QUERY.format(query=table, email=request['emailId'],listid =request['listId'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True
            # ADHOC ACCOUNT LEVEL CHECKING
                logger.info("Executing query ::: SELECT COUNT(1) FROM CUST_REPORT_DB_QA.APT_UNSUB_DETAILS_OTEAM  where if(listid='2','3188',listid)=if('{listid}'='2','3188','{listid}') AND EMAIL = '{email}'".format(email=request['emailId'], listid=request['listId']))
                result = mdb_session.execute(text("SELECT COUNT(1) FROM CUST_REPORT_DB_QA.APT_UNSUB_DETAILS_OTEAM  where if(listid='2','3188',listid)=if('{listid}'='2','3188','{listid}') AND EMAIL = '{email}'".format(email=request['emailId'], listid=request['listId'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True

                logger.info("Executing query ::: select count(1) from  CUST_REPORT_DB_QA.APT_UNSUB_DETAILS_OTEAM a where a.email = '{email}' and a.listid in ( select listid from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE where account_name in (select account_name from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE  where listid = '{listid}') )".format(email=request['emailId'], listid=request['listId']))
                result = mdb_session.execute(text("select count(1) from  CUST_REPORT_DB_QA.APT_UNSUB_DETAILS_OTEAM a where a.email = '{email}' and a.listid in ( select listid from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE where account_name in (select account_name from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE  where listid = '{listid}') )".format(email=request['emailId'], listid=request['listId'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True

                logger.info("Executing query ::: select count(1) from (select email,listid from  CUST_REPORT_DB_QA.APT_EMAIL_REPLIES_TRANSACTIONAL a join INFS_DB_QA.INFS_ADHOC_DOMAINS b on lower(trim(a.domain))=lower(trim(b.domain)) ) G where G.LISTID in ( select listid from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE where account_name in (select account_name from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE  where listid = '{listid}') ) and G.EMAIL = '{email}'".format(email=request['emailId'], listid=request['listId']))
                result = mdb_session.execute(text("select count(1) from (select email,listid from  CUST_REPORT_DB_QA.APT_EMAIL_REPLIES_TRANSACTIONAL a join INFS_DB_QA.INFS_ADHOC_DOMAINS b on lower(trim(a.domain))=lower(trim(b.domain)) ) G where G.LISTID in ( select listid from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE where account_name in (select account_name from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE  where listid = '{listid}') ) and G.EMAIL = '{email}'".format(email=request['emailId'], listid=request['listId'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True

                logger.info("Executing query ::: SELECT COUNT(1) FROM INFS_DB.INFS_UNSUBS_ACCOUNT_WISE a where a.email = '{email}' and a.listid in ( select listid from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE where account_name in ( select account_name from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE  where listid = '{listid}'))".format(email=request['emailId'], listid=request['listId']))
                result = mdb_session.execute(text("SELECT COUNT(1) FROM INFS_DB.INFS_UNSUBS_ACCOUNT_WISE a where a.email = '{email}' and a.listid in ( select listid from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE where account_name in ( select account_name from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE  where listid = '{listid}'))".format(email=request['emailId'], listid=request['listId'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True

                logger.info("Executing query ::: SELECT COUNT(1) FROM INFS_DB.APT_INFS_ACCOUNT_LEVEL_STATIC_SUPPRESSION_DATA G where G.listid in (select listid from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE where account_name in ( select account_name from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE  where listid = '{listid}')) and G.EMAIL ='{email}'".format(email=request['emailId'], listid=request['listId']))
                result = mdb_session.execute(text("SELECT COUNT(1) FROM INFS_DB.APT_INFS_ACCOUNT_LEVEL_STATIC_SUPPRESSION_DATA G where G.listid in (select listid from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE where account_name in ( select account_name from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE  where listid = '{listid}')) and G.EMAIL ='{email}'".format(email=request['emailId'], listid=request['listId'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True

                logger.info("Executing query ::: SELECT COUNT(1) FROM INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION G where G.email= '{email} and '{listid}' in (select listid from INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION_LISTIDS)".format(email=request['emailId'], listid=request['listId']))
                result = mdb_session.execute(text("SELECT COUNT(1) FROM INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION G where G.email= '{email}' and '{listid}' in (select listid from INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION_LISTIDS)".format(email=request['emailId'], listid=request['listId'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True

                logger.info("Executing query ::: SELECT COUNT(1) FROM INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION G where G.email= '{email}' and '{listid}' in (select listid from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE where account_name in (select account_name from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE where listid in (select  listid from INFS_DB_QA.BLUE_CLIENT_DATA_SUPPRESSION_LISTIDS)))".format(email=request['emailId'], listid=request['listId']))
                result = mdb_session.execute(text("SELECT COUNT(1) FROM INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION G where G.email= '{email}' and '{listid}' in (select listid from INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE where account_name in (select account_name from  INFS_DB_QA.INFS_ORANGE_MAPPING_TABLE where listid in (select  listid from INFS_DB_QA.BLUE_CLIENT_DATA_SUPPRESSION_LISTIDS)))".format(email=request['emailId'], listid=request['listId'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True
            return False
    except Exception as e:
        logger.info(f"Exception occurred while checking on Feed level Suppression. Could you please look into this record {request}")
        logger.error(f"Exception details are ::: {str(e) + traceback.format_exc()}")
        raise  Exception(f"Exception occurred while checking on Feed level Suppression. ED :: {str(e)}")


def updatePostTransactionStatus(logger, table_name, request, status, ed=''):
    try:
        with MySQLSessionManager(logger, MDB_MYSQL_CONFIGS) as mdb_session:
            logger.info(f" Executing query ::: {UPDATE_POST_PROCESSING_TABLE_STATUS_QUERY.format(table_name=table_name, id=request['id'], status=status,errorReason=ed)}")
            mdb_session.execute(text(UPDATE_POST_PROCESSING_TABLE_STATUS_QUERY.format(table_name=table_name, id=request['id'], status=status, errorReason =ed )))
            logger.info(f"{table_name} status updated to '{status}' for the record {request}")
    except Exception as e:
        logger.error(f"Exception Occurred while updating status for table {table_name} with status {status} for record  {request}")
        logger.error(f"Please look into this error ::: {str(e) + traceback.format_exc()}")
        raise Exception(f"Exception Occurred while updating status for table {table_name} with status {status} for record  {request}")

def updateTargetListId(logger, table_name, request, target_id):
    try:
        with MySQLSessionManager(logger, MDB_MYSQL_CONFIGS) as mdb_session:
            channel = request['channel']
            if channel == 'GREEN':
                record_listid = request['listId']
                listid_column = "listId"
            elif channel == "ORANGE" or channel == "INFS":
                record_listid = request['plistId']
                listid_column = "plistId"
            logger.info("Fetching target_listid info from the Target Table...")
            logger.info(f"Executing query ::: {GET_TARGET_LISTID_INFO_QUERY.format(listidcolumn = listid_column ,targetid = target_id , channel = request['channel'])}")
            result = mdb_session.execute(text(GET_TARGET_LISTID_INFO_QUERY.format(listidcolumn = listid_column ,targetid = target_id , channel = request['channel'])))
            target_listid = result.fetchone()[0]
            logger.info(f"Fetched target_listid : {target_listid} for {listid_column} : {record_listid} ")
            request['targetListId'] = target_listid
            request['targetId'] = target_id
            logger.info(f" Executing query ::: {UPDATE_POST_PROCESSING_TABLE_TARGET_LISTID_QUERY.format(table_name=table_name, targetId=target_id,  targetListId=request['targetListId'], id=request['id'])}")
            mdb_session.execute(text(UPDATE_POST_PROCESSING_TABLE_TARGET_LISTID_QUERY.format(table_name=table_name, targetId=target_id, targetListId=request['targetListId'], id=request['id'])))
            logger.info(f"{table_name} targetListId updated to '{request['targetListId']}' for the record {request}")
            return target_listid
    except Exception as e:
        logger.error(f"Exception Occurred while updating targetListId for table {table_name} with targetListId '{request['targetListId']}' for record  {request}")
        logger.error(f"Please look into this error ::: {str(e) + traceback.format_exc()}")

# Function to get quota usage for each target for a given source
def get_quota_usage_for_targets(source_id ,logger):
    logger.info("Method get_quota_usage_for_targets is invoked..")
    with MySQLSessionManager(logger, MDB_MYSQL_CONFIGS) as mdb_session:
        logger.info(f"Executing query ::: {FETCH_TARGET_QUOTA_DETAILS.format(sourceid=source_id)}")
        targets = mdb_session.execute(text(FETCH_TARGET_QUOTA_DETAILS.format(sourceid=source_id)))
        targets_as_dicts = [dict(zip(targets.keys(), row)) for row in targets.fetchall()]
        logger.info(f"Fetched targets for sourceid - {source_id} ")
        logger.info(f"Target details :: {targets_as_dicts}")
        return targets_as_dicts


# Function to update the quota check table after distributing a record
def update_quota_check_table(target_id, quota_type,logger):
    logger.info("Method update_quota_check_table is invoked...  ")
    with MySQLSessionManager(logger, MDB_MYSQL_CONFIGS) as mdb_session:
        query = "INSERT INTO {QUOTA_CHECK_TABLE} (targetId, count, hour, deployedDate) VALUES ({targetID}, 1, HOUR(NOW()), CURDATE()) ON DUPLICATE KEY UPDATE count = count + 1 "
        logger.info(f"Executing query ::: {query.format(QUOTA_CHECK_TABLE= QUOTA_CHECK_TABLE,targetID=target_id)}")
        mdb_session.execute(query.format(QUOTA_CHECK_TABLE=QUOTA_CHECK_TABLE , targetID=target_id))
    logger.info(f"Successfully updated {QUOTA_CHECK_TABLE} table ")


lock = threading.Lock()

# Function to process a single record and assign it to a target based on quota availability
def quota_check(request, logger):
    with lock:
        # Fetch current quota usage for each target
        logger.info(f"quota_check process started")
        targets = get_quota_usage_for_targets(request['sourceId'], logger)
        # Calculate available quotas for each target
        available_targets = []
        for target in targets:
            target['availableQuota'] = max(0, target['quota'] - target['currentCount'])
            if target['availableQuota'] > 0:
                available_targets.append(target)
        if not available_targets:
            logger.info("No available target with quota for record.")
            return None  # No available targets
        # Sort the targets by available quota (to distribute fairly)
        available_targets.sort(key=lambda t: t['availableQuota'], reverse=True)
        # Assign the record to the first target with available quota
        selected_target = available_targets[0]
        # Update the quota check for the selected target
        update_quota_check_table(selected_target['targetId'], selected_target['quotaType'],logger)
        logger.info(f"quota_check process ended")
        return selected_target['targetId']


def hitTheAPI(logger, request,table_name):
    try:
        with MySQLSessionManager(logger, MDB_MYSQL_CONFIGS) as mdb_session:
            logger.info("Fetching Transactional table info from the Source Table...")
            logger.info(f"Executing query ::: {GET_TRANSACTIONAL_TABLE_INFO_QUERY.format(sourceId=request['sourceId'])}")
            result = mdb_session.execute(text(GET_TRANSACTIONAL_TABLE_INFO_QUERY.format(sourceId=request['sourceId'])))
            transactionalTable = result.fetchone()[0]
            logger.info(f"Fetching API url from target table... ")
            logger.info(f"Executing query ::: {FETCH_TARGET_API_URL_QUERY.format(targetId=request['targetId'])} ")
            result = mdb_session.execute(text(FETCH_TARGET_API_URL_QUERY.format(targetId=request['targetId'])))
            result = result.fetchone()
            if result is None:
                raise Exception(f"Unable to find API url and mapping columns from {TARGET_DETAILS_TABLE}.")
            api_url_dict = dict(zip(result.keys(), result))
            logger.info(f"Now need to fetch the api params from Transactional table {transactionalTable}")
            logger.info(f"Executing query ::: {FETCH_DATA_FROM_TRANSACTIONAL_QUERY.format(columns = api_url_dict['urlParamFieldMapping'],table_name=transactionalTable,transactionalId= request['transactionalId'])}")
            result = mdb_session.execute(text(FETCH_DATA_FROM_TRANSACTIONAL_QUERY.format(columns = api_url_dict['urlParamFieldMapping'],table_name=transactionalTable,transactionalId= request['transactionalId'])))
            result = result.fetchone()
            if result is None:
                raise Exception(f"Unable to find this record in {transactionalTable}.")
            api_params_as_dict = dict(zip(result.keys(), result))
            logger.info(f"Fetched API params from Transactional table. ::: {api_params_as_dict}")
            API = api_url_dict['apiURL']
            formatted_url = API.format(**api_params_as_dict)
            logger.info(f"Here is formatted url ::: {formatted_url}")
            response = requests.get(formatted_url)
            apiResponse = f"Code:: {response.status_code} - Response:: {response.text}".replace('"',"").replace("'",'')
            logger.info(f"Executing query :: {UPDATE_POST_PROCESSING_TABLE_API_QUERY.format(table_name=table_name,apiCall=formatted_url,apiResponse=apiResponse,id=request['id'])}")
            mdb_session.execute(UPDATE_POST_PROCESSING_TABLE_API_QUERY.format(table_name=table_name,apiCall=formatted_url,apiResponse=apiResponse,id=request['id']))
            if response.status_code == 200:
                logger.info("API call successful!")
                logger.info(apiResponse)
                return True
            else:
                logger.info(f"API call failed with status code {response.status_code}")
                return False
    except Exception as e:
        logger.error("Exception occurred at step Hitting the API...")
        logger.error(f"Please look into this.... {str(e) + traceback.format_exc()}")
        raise Exception(f"Exception occurred at step Hitting the API.  ED :: {str(e)}")

