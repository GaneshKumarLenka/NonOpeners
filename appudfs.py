
from serviceconfigurations import *
from basicudfs import *


def getWaitingRecords(table_name, logger):
    try:
        logger.info(f"Method getWaitingRecords from {table_name} Table invoked...")
        mdb_session = createMysqlConnectionSession(logger, MDB_MYSQL_CONFIGS)
        logger.info(f"Executing query ::: {FETCH_WAITING_RECORDS_QUERY.format(table_name=table_name)}")
        waiting_records = mdb_session.execute(text(FETCH_WAITING_RECORDS_QUERY.format(table_name=table_name)))
        waiting_records_as_dicts = [dict(zip(waiting_records.keys(), row)) for row in waiting_records.fetchall()]
        logger.info(f"Fetched Waiting records for the table  {table_name} ")
        logger.info(f"Sample records are :: {waiting_records_as_dicts[:3]}")
        return waiting_records_as_dicts
    except Exception as e:
        logger.error(f"Unable to Get Waiting requests for the table .. {table_name}")
        logger.error(f"Please look into this..{str(e)}")
        raise Exception(f"Unable to Get Waiting requests for the table .. {table_name}")
    finally:
        if mdb_session:
            mdb_session.close()



def isResponderOrNot(request, thread_logger):
    try:
        thread_logger.info(f"Method invoked To check record is responder or not in last 24 hrs...")
        mdb_session = createMysqlConnectionSession(thread_logger,MDB_MYSQL_CONFIGS)
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
        thread_logger.info(f"Exception occurred while checking for responder or not::: Please look into this... {request} ::: Error reason :::{str(e)}")
        return False
    finally:
        if mdb_session:
            mdb_session.close()






def isDeliveredOrNOt(request,thread_logger):
    thread_logger.info(f"Method invoked to check record is Delivered or not in last 24 hrs...")
    jbdb_session = createMysqlConnectionSession(thread_logger,JBDB4_MYSQL_CONFIGS)
    thread_logger.info(f" Executing query ::: {CHECK_FOR_DELIVERED_QUERY.format(subid=request['subid'],listid=request['listid'])}")
    result = jbdb_session.execute(text(CHECK_FOR_DELIVERED_QUERY.format(subid=request['subid'],listid=request['listid'])))
    pass

def isFeedlevelSuppressedOrNot(request,thread_logger):
    thread_logger.info(f"Method invoked to check the record is Feed Level Suppressed or not...")
    channel = request['channel']
    if channel == 'GREEN':
        checkInGreenFeedSupp()
    elif channel == 'INFS' or channel == 'ORANGE':
        checkInInfsFeedSupp()
    else:
        thread_logger.info(f"Unknown channel record fetched..Please look into this {request} ")

def checkInGreenFeedSupp():
    pass
def checkInInfsFeedSupp():
    pass



def updatePostTransactionStatus(logger, table_name, status):
    try:
        pass
    except Exception as e:
        pass


