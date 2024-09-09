from serviceconfigurations import *
from basicudfs import *
from appudfs import *


class Postprocessing:
    def __init__(self):
        self.request_queue = Queue()

    def processTable(self, table_name):
        try:
            table_logger = create_logger(table_name)
            table_logger.info(f"Processing table {table_name} started ... {time.ctime()}")
            with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
                futures = {}
                while True:
                    try:
                        waitingRequests = getWaitingRecords(table_name, table_logger)
                        if waitingRequests:
                            table_logger.info(f"Waiting Requests are :: {waitingRequests[:3]}")
                            for request in waitingRequests:
                                self.request_queue.put(request)
                                table_logger.info(f" Request Sent to Queue :: request details {request}")
                            while not self.request_queue.empty():
                                if len(futures) < THREAD_COUNT:
                                    thread_number = len(futures) + 1
                                    future = executor.submit(self.processRequests,table_name, thread_number)
                                    futures[future] = thread_number
                                    table_logger.info(f"Request submitted to Thread-{thread_number} from Executor....")
                                else:
                                    table_logger.info("No thread is free, waiting for a thread to complete...")
                                    completed_futures, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
                                    for completed_future in completed_futures:
                                        thread_number = futures.pop(completed_future)
                                        logger.info(f"Thread-{thread_number} has completed.")
                        else:
                            table_logger.info(f"No waiting requests found ....")
                        time.sleep(30)
                    except Exception as e:
                        table_logger.error(f"Error in ThreadPoolExecutor loop() :: {e}")
                        table_logger.error(traceback.format_exc())
                        time.sleep(30)
        except Exception as e:
            logger.error(f"Error in requestThreadProcess() :: {e}")
            logger.error(traceback.format_exc())

    def processRequests(self, table_name, thread_number):
        thread_logger = create_logger(f"Thread_{table_name}_{thread_number}")
        request = self.request_queue.get()
        thread_logger.info(f"[Thread-{thread_number}] Request fetched from Queue ::: {request}")
        if request is None:
            logger.info(f"[Thread-{thread_number}] No request found... Closing this Thread...")
            self.request_queue.task_done()
            return
        thread_logger.info(f"[Thread-{thread_number}] Request Processing Started .. {time.ctime()}")
        thread_logger.info(f"[Thread-{thread_number}] Checking It Was Responder or Not")
        if isResponderOrNot(request,thread_logger):
            thread_logger.info(f"Found this record {request}in Responder Table. Updating status to 'R'...")
            updatePostTransactionStatus(thread_logger,table_name,request,'R')
            self.request_queue.task_done()
            return
        if not isDeliveredOrNOt(request,thread_logger):
            thread_logger.info(f"Unable to find this record {request} in Delivered Table. Updating status to ''...")
            updatePostTransactionStatus(thread_logger, table_name, request, '')
            self.request_queue.task_done()
            return
        if isFeedlevelSuppressedOrNot(request,thread_logger):
            thread_logger.info(f"Found this record {request} in Feed Level Suppression Tables. Updating status to 'Z'...")
            updatePostTransactionStatus(thread_logger, table_name, request, 'Z')
            self.request_queue.task_done()
            return





if __name__ == '__main__':
    logger = create_logger("PostProcessing_main")
    logger.info(f"Service execution started ::: {time.ctime()}")
    logger.info("Getting Tables Info..")
    logger.info(f"We have these tables .. {POST_PROCESSING_TABLES}")
    with ThreadPoolExecutor(max_workers=len(POST_PROCESSING_TABLES)) as executor:
        futures = {}
        try:
            for table_name in POST_PROCESSING_TABLES:
                future = executor.submit(Postprocessing.processTable, table_name)
                futures[future] = table_name
                logger.info(f"{table_name} submitted to Thread-{table_name} from Executor....")

            completed_futures, _ = wait(futures.keys())
            for completed_future in completed_futures:
                table_name = futures.pop(completed_future)
                logger.info(f"Thread- {table_name} has completed.")
        except Exception as e:
            logger.error(f"Error in ThreadPoolExecutor loop() :: {e}")
            logger.error(traceback.format_exc())
            time.sleep(5)
    logger.info(f"Service execution ended ::: {time.ctime()}")
