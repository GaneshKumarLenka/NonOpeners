from serviceconfigurations import *
from basicudfs import *


def getWaitingRecords(table_name, logger):
    try:
        logger.info(f"Method getWaitingRecords from {table_name} Table invoked...")
        with createMysqlConnectionSession(logger, MDB_MYSQL_CONFIGS) as mdb_session:
            logger.info(f"Executing query ::: {FETCH_WAITING_RECORDS_QUERY.format(table_name=table_name)}")
            waiting_records = mdb_session.execute(text(FETCH_WAITING_RECORDS_QUERY.format(table_name=table_name)))
            waiting_records_as_dicts = [dict(zip(waiting_records.keys(), row)) for row in waiting_records.fetchall()]
            logger.info(f"Fetched Waiting records for the table  {table_name} ")
            logger.info(f"Sample records are :: {waiting_records_as_dicts[:3]}")
            return waiting_records_as_dicts
    except Exception as e:
        logger.error(f"Unable to Get Waiting requests for the table .. {table_name}")
        logger.error(f"Please look into this..{str(e) + traceback.format_exc()}")
        raise Exception(f"Unable to Get Waiting requests for the table .. {table_name}")


def isResponderOrNot(request, thread_logger):
    try:
        thread_logger.info(f"Method invoked To check record is responder or not in last 24 hrs...")
        with createMysqlConnectionSession(thread_logger, MDB_MYSQL_CONFIGS) as mdb_session:
            channel = request['channel']
            if channel == 'GREEN':
                open_table = GREEN_OPEN_TABLE
            elif channel == 'INFS' or channel == 'ORANGE':
                open_table = INFS_OPEN_TABLE
            else:
                thread_logger.info(f"Unable to find this record any channel.Please look into this record ::: {request}")
                return False
            thread_logger.info(f"Executing query ::: {CHECK_FOR_RESPONDER_QUERY.format(open_table, request['subid'], request['profileid'])}")
            result = mdb_session.execute(text(CHECK_FOR_RESPONDER_QUERY.format(open_table, request['subid'], request['profileid'])))
            result = result.fetchone()[0]
            if result >= 1:
                return True
            return False
    except Exception as e:
        thread_logger.info(f"Exception occurred while checking for responder or not::: Please look into this... {request} ::: Error reason :::{str(e) + traceback.format_exc()}")
        return False


def isDeliveredOrNOt(request, thread_logger):
    try:
        thread_logger.info(f"Method invoked to check record is Delivered or not in last 24 hrs...")
        with createMysqlConnectionSession(thread_logger, JBDB4_MYSQL_CONFIGS) as jbdb_session:
            thread_logger.info(f" Executing query ::: {CHECK_FOR_DELIVERED_QUERY.format(subid=request['subid'], email=request['email'])}")
            result = jbdb_session.execute(text(CHECK_FOR_DELIVERED_QUERY.format(subid=request['subid'], email=request['email'])))
            result = result.fetchone()[0]
            if result >= 1:
                return True
            return False
    except Exception as e:
        thread_logger.info(f"Exception occurred while checking for Delivered or not::: Please look into this... {request} ::: Error reason :::{str(e) + traceback.format_exc()}")
        return False


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
        with createMysqlConnectionSession(logger, MDB_MYSQL_CONFIGS) as mdb_session:
            logger.info(f"Checking in Green Feed Level Suppression's... ")
            for table in GREEN_FEED_LEVEL_SUPP_TABLES['email']:
                logger.info(f"Checking the record in Suppressed or not in table {table}")
                logger.info(f"Executing query ::: {CHECK_GREEN_FEED_SUPP_EMAIL_LEVEL_QUERY.format(table_name=table, email=request['email'])}")
                result = mdb_session.execute(text(CHECK_GREEN_FEED_SUPP_EMAIL_LEVEL_QUERY.format(table_name=table, email=request['email'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True
            for table in GREEN_FEED_LEVEL_SUPP_TABLES['email_listid']:
                logger.info(f"Checking the record in Suppressed or not in table {table}")
                logger.info(f"Executing query ::: {CHECK_GREEN_FEED_SUPP_EMAIL_LISTID_LEVEL_QUERY.format(table_name=table, email=request['email'],listid =request['listid'] )}")
                result = mdb_session.execute(text(CHECK_GREEN_FEED_SUPP_EMAIL_LEVEL_QUERY.format(table_name=table, email=request['email'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True
            return False
    except Exception as e:
        logger.info(f"Exception occurred while checking on Feed level Suppression. Could you please look into this record {request}")
        logger.error(f"Exception details are ::: {str(e) + traceback.format_exc()}")
        return False


def checkInInfsFeedSupp(request, logger):
    try:
        with createMysqlConnectionSession(logger, MDB_MYSQL_CONFIGS) as mdb_session:
            logger.info(f"Checking in Infs Feed Level Suppression's... ")
            for table in INFS_FEED_LEVEL_SUPP_TABLES['email']:
                logger.info(f"Checking the record in Suppressed or not in table {table}")
                logger.info(f"Executing query ::: {CHECK_INFS_FEED_SUPP_EMAIL_LEVEL_QUERY.format(table_name=table, email=request['email'])}")
                result = mdb_session.execute(text(CHECK_INFS_FEED_SUPP_EMAIL_LEVEL_QUERY.format(table_name=table, email=request['email'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True
            for table in INFS_FEED_LEVEL_SUPP_TABLES['email_listid']:
                logger.info(f"Checking the record in Suppressed or not in table {table}")
                logger.info(f"Executing query ::: {CHECK_INFS_FEED_SUPP_EMAIL_LISTID_LEVEL_QUERY.format(table_name=table, email=request['email'],listid =request['listid'] )}")
                result = mdb_session.execute(text(CHECK_INFS_FEED_SUPP_EMAIL_LISTID_LEVEL_QUERY.format(table_name=table, email=request['email'])))
                result = result.fetchone()[0]
                if result >= 1:
                    return True
            # ADHOC ACCOUNT LEVEL CHECKING
            logger.info("Executing query ::: SELECT COUNT(1) FROM CUST_REPORT_DB.APT_UNSUB_DETAILS_OTEAM  where iff(listid='2','3188',list_id)=iff('{listid}'='2','3188','{listid}') AND EMAIL = {email}".format(email=request['email'],listid=request['listid']))
            result = mdb_session.execute(text("SELECT COUNT(1) FROM CUST_REPORT_DB.APT_UNSUB_DETAILS_OTEAM  where iff(listid='2','3188',list_id)=iff('{listid}'='2','3188','{listid}') AND EMAIL = {email}".format(email=request['email'],listid=request['listid'])))
            result = result.fetchone()[0]
            if result >= 1:
                return True

            logger.info("Executing query ::: SELECT COUNT(1) FROM (select c.email,d.account_name from CUST_REPORT_DB.APT_UNSUB_DETAILS_OTEAM c join INFS_DB.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) G where G.account_name={account_name} and G.EMAIL = {email}".format(email=request['email'], account_name=request['accountname']))
            result = mdb_session.execute(text("SELECT COUNT(1) FROM (select c.email,d.account_name from CUST_REPORT_DB.APT_UNSUB_DETAILS_OTEAM c join INFS_DB.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) G where G.account_name={account_name} and G.EMAIL = {email}".format(email=request['email'], account_name=request['accountname'])))
            result = result.fetchone()[0]
            if result >= 1:
                return True

            logger.info("Executing query ::: select count(1) from  (select c.email,d.account_name from (select email,listid from  CUST_REPORT_DB.APT_EMAIL_REPLIES_TRANSACTIONAL a join INFS_DB.INFS_ADHOC_DOMAINS b on lower(trim(a.domain))=lower(trim(b.domain)) where a.id > 17218326 ) c join INFS_DB.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid ) G where G.account_name={account_name} and G.EMAIL = {email}".format(email=request['email'], account_name=request['accountname']))
            result = mdb_session.execute(text("select count(1) from  (select c.email,d.account_name from (select email,listid from  CUST_REPORT_DB.APT_EMAIL_REPLIES_TRANSACTIONAL a join INFS_DB.INFS_ADHOC_DOMAINS b on lower(trim(a.domain))=lower(trim(b.domain)) where a.id > 17218326 ) c join INFS_DB.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid ) G where G.account_name={account_name} and G.EMAIL = {email}".format(email=request['email'], account_name=request['accountname'])))
            result = result.fetchone()[0]
            if result >= 1:
                return True

            logger.info("Executing query ::: SELECT COUNT(1) FROM (select c.email,d.account_name from INFS_DB.INFS_UNSUBS_ACCOUNT_WISE c join INFS_DB.INFS_ORANGE_MAPPING_TABLE d on c.account_name=d.account_name ) G where G.account_name= {account_name} and G.EMAIL= {email}".format(email=request['email'], account_name=request['accountname']))
            result = mdb_session.execute(text("SELECT COUNT(1) FROM (select c.email,d.account_name from INFS_DB.INFS_UNSUBS_ACCOUNT_WISE c join INFS_DB.INFS_ORANGE_MAPPING_TABLE d on c.account_name=d.account_name ) G where G.account_name= {account_name} and G.EMAIL= {email}".format(email=request['email'], account_name=request['accountname'])))
            result = result.fetchone()[0]
            if result >= 1:
                return True

            logger.info("Executing query ::: SELECT COUNT(1) FROM (select c.email,d.account_name from INFS_DB.APT_INFS_ACCOUNT_LEVEL_STATIC_SUPPRESSION_DATA c join INFS_DB.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) G where G.account_name= {account_name} and G.EMAIL ={email}".format(email=request['email'], account_name=request['accountname']))
            result = mdb_session.execute(text("SELECT COUNT(1) FROM (select c.email,d.account_name from INFS_DB.APT_INFS_ACCOUNT_LEVEL_STATIC_SUPPRESSION_DATA c join INFS_DB.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) G where G.account_name= {account_name} and G.EMAIL ={email}".format(email=request['email'], account_name=request['accountname'])))
            result = result.fetchone()[0]
            if result >= 1:
                return True

            logger.info("Executing query ::: SELECT COUNT(1) FROM INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION G where G.email= {email} and {listid} in (select listid from INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION_LISTIDS)".format(email=request['email'], listid=request['listid']))
            result = mdb_session.execute(text("SELECT COUNT(1) FROM INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION G where G.email= {email} and {listid} in (select listid from INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION_LISTIDS)".format(email=request['email'], listid=request['listid'])))
            result = result.fetchone()[0]
            if result >= 1:
                return True

            logger.info("Executing query ::: SELECT COUNT(1) FROM INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION G where G.email= {email} and {account_name} in (select account_name from INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION_LISTIDS c join INFS_DB.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid)".format(email=request['email'], account_name=request['accountname']))
            result = mdb_session.execute(text("SELECT COUNT(1) FROM INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION G where G.email= {email} and {account_name} in (select account_name from INFS_DB.BLUE_CLIENT_DATA_SUPPRESSION_LISTIDS c join INFS_DB.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid)".format(email=request['email'], account_name=request['accountname'])))
            result = result.fetchone()[0]
            if result >= 1:
                return True
            return False
    except Exception as e:
        logger.info(f"Exception occurred while checking on Feed level Suppression. Could you please look into this record {request}")
        logger.error(f"Exception details are ::: {str(e) + traceback.format_exc()}")
        return False


def updatePostTransactionStatus(logger, table_name, request, status):
    try:
        with createMysqlConnectionSession(logger, MDB_MYSQL_CONFIGS) as mdb_session:
            logger.info(f" Executing query ::: {UPDATE_POST_PROCESSING_TABLE_STATUS_QUERY.format(table_name=table_name, subid=request['subid'], profileid=request['profileid'], status=status)}")
            mdb_session.execute(text(UPDATE_POST_PROCESSING_TABLE_STATUS_QUERY.format(table_name=table_name, subid=request['subid'],profileid=request['profileid'], status=status)))
            logger.info(f"{table_name} status updated to '{status}' for the record {request}")
    except Exception as e:
        logger.error(f"Exception Occurred while updating status for table {table_name} with status {status} for record  {request}")
        logger.error(f"Please look into this error ::: {str(e) + traceback.format_exc()}")

def updateTargetListId(logger, table_name, request, target_id):
    try:
        with createMysqlConnectionSession(logger, MDB_MYSQL_CONFIGS) as mdb_session:
            channel = request['channel']
            if channel == 'GREEN':
                record_listid = request['listid']
                listid_column = "listid"
            elif channel == "ORANGE" or channel == "INFS":
                record_listid = request['plistid']
                listid_column = "plistid"
            logger.info("Fetching target_listid info from the Target Table...")
            logger.info(f"Executing query ::: {GET_TARGET_LISTID_INFO_QUERY.format(listidcolumn = listid_column ,targetid = target_id , channel = request['channel'])}")
            result = mdb_session.execute(text(GET_TARGET_LISTID_INFO_QUERY.format(listidcolumn = listid_column ,targetid = target_id , channel = request['channel'])))
            target_listid = result.fetchone()[0]
            logger.info(f"Fetched target_listid : {target_listid} for {listid_column} : {record_listid} ")
            request['targetListId'] = target_listid
            logger.info(f" Executing query ::: {UPDATE_POST_PROCESSING_TABLE_TARGET_LISTID_QUERY.format(table_name=table_name, targetListId=request['targetListId'], id=request['id'])}")
            mdb_session.execute(text(UPDATE_POST_PROCESSING_TABLE_STATUS_QUERY.format(table_name=table_name, targetListId=request['targetListId'], id=request['id'])))
            logger.info(f"{table_name} targetListId updated to '{request['targetListId']}' for the record {request}")
            return target_listid
    except Exception as e:
        logger.error(f"Exception Occurred while updating targetListId for table {table_name} with targetListId '{request['targetListId']}' for record  {request}")
        logger.error(f"Please look into this error ::: {str(e) + traceback.format_exc()}")

# Function to get quota usage for each target for a given source
def get_quota_usage_for_targets(source_id):
    with createMysqlConnectionSession(logger, MDB_MYSQL_CONFIGS) as mdb_session:
        targets = mdb_session.execute(text(FETCH_TARGET_QUOTA_DETAILS.format(sourceid=source_id)))
        targets_as_dicts = [dict(zip(waiting_records.keys(), row)) for row in targets.fetchall()]
        logger.info(f"Fetched targets for sourceid - {source_id} ")
        logger.info(f"Target details :: {targets_as_dicts}")
        return targets_as_dicts


# Function to update the quota check table after distributing a record
def update_quota_check_table(target_id, quota_type):
    with createMysqlConnectionSession(logger, MDB_MYSQL_CONFIGS) as mdb_session:
        if quota_type == 'H':
            query = f'''
            INSERT INTO {QUOTA_CHECK_TABLE} (targetId, count, hour, deployedDate)
            VALUES (%s, 1, HOUR(NOW()), CURDATE())
            ON DUPLICATE KEY UPDATE count = count + 1;
            '''
        else:
            query = f'''
            INSERT INTO {QUOTA_CHECK_TABLE} (targetId, count, deployedDate)
            VALUES (%s, 1, CURDATE())
            ON DUPLICATE KEY UPDATE count = count + 1;
            '''
        mdb_session.execute(query, (target_id,))
    logger.info(f"Successfully updated {QUOTA_CHECK_TABLE} table ")



# Function to process a single record and assign it to a target based on quota availability
def quota_check(request, logger):
    with lock:
        # Fetch current quota usage for each target
        logger.info(f"quota_check process started")
        targets = get_quota_usage_for_targets(request['sourceId'])

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
        update_quota_check_table(selected_target['targetId'], selected_target['quotaType'])


        logger.info(f"quota_check process ended")

        return selected_target['targetId']


def hitTheAPI(logger,request):
    try:
        channel =request['channel']
        if channel == 'GREEN':
            record_listid = request['listid']
            listid_column = "listid"
        elif channel == "ORANGE" or channel == "INFS":
            record_listid = request['plistid']
            listid_column = "plistid"
        with createMysqlConnectionSession(logger, MDB_MYSQL_CONFIGS) as mdb_session:
            logger.info("Fetching Transactional table info from the Source Table...")
            logger.info(f"Executing query ::: {GET_TRANSACTIONAL_TABLE_INFO_QUERY.format(listidcolumn = listid_column ,listid = record_listid , channel = request['channel'])}")
            result = mdb_session.execute(text(GET_TRANSACTIONAL_TABLE_INFO_QUERY.format(listidcolumn = listid_column ,listid = record_listid , channel = request['channel'])))
            transactionalTable = result.fetchone()[0]
            logger.info(f"Now need to fetch the api params from Transactional table {transactionalTable}")


    except Exception as e:
        logger.error("Exception occurred at step Hitting the API...")
        logger.error(f"Please look into this.... {str(e) + traceback.format_exc()}")