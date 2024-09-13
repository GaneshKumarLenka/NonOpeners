from serviceconfigurations import *
from basicudfs import *
from appudfs import *


class Postprocessing:
    def __init__(self):
        self.request_queue = Queue()
    def post_process(self):
        logger = create_logger("PostProcessing_main")
        logger.info(f"Service execution started ::: {time.ctime()}")
        logger.info("Getting POST_PROCESSING_TABLE  Info..")
        logger.info(f"We have thee table .. {POST_PROCESSING_TABLE}")
        table_name = POST_PROCESSING_TABLE
        self.processTable(table_name)
        logger.info(f"Service execution ended ::: {time.ctime()}")
    def processTable(self, table_name):
        try:
            table_logger = create_logger("WR_picker")
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
                                    future = executor.submit(self.processRequests,table_name, thread_number,table_logger)
                                    futures[future] = thread_number
                                    table_logger.info(f"Request submitted to Thread-{thread_number} from Executor....")
                                else:
                                    table_logger.info("No thread is free, waiting for a thread to complete...")
                                    completed_futures, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
                                    for completed_future in completed_futures:
                                        thread_number = futures.pop(completed_future)
                                        table_logger.info(f"Thread-{thread_number} has completed.")
                        else:
                            table_logger.info(f"No waiting requests found ....")
                        time.sleep(30)
                    except Exception as e:
                        table_logger.error(f"Error in ThreadPoolExecutor loop() :: {e}")
                        table_logger.error(traceback.format_exc())
                        time.sleep(30)
        except Exception as e:
            table_logger.error(f"Error in requestThreadProcess() :: {e}")
            table_logger.error(traceback.format_exc())

    def processRequests(self, table_name, thread_number, picker_logger):
        try:
            request = self.request_queue.get()
            picker_logger.info(f"[Thread-{thread_number}] Request fetched from Queue ::: {request}")
            if request is None:
                picker_logger.info(f"[Thread-{thread_number}] No request found... Closing this Thread...")
                return
            thread_logger = create_logger(f"Thread_{thread_number}")
            thread_logger.info(f"[Thread-{thread_number}] Request Processing Started .. {time.ctime()}")
            thread_logger.info("Making record with 'I' - In progress state after picking..")
            updatePostTransactionStatus(logger=thread_logger, table_name=table_name, request=request, status='I')
            thread_logger.info(f"[Thread-{thread_number}] Checking It Was Responder or Not")
            if isResponderOrNot(request,thread_logger):
                thread_logger.info(f"Found this record {request}in Responder Table. Updating status to 'R'...")
                updatePostTransactionStatus(logger=thread_logger, table_name=table_name, request=request, status='R')
                return
            '''if not isDeliveredOrNOt(request,thread_logger):
                thread_logger.info(f"Unable to find this record {request} in Delivered Table. Updating status to 'U'...")
                updatePostTransactionStatus(thread_logger, table_name, request, 'U')
                return'''
            if isFeedlevelSuppressedOrNot(request,thread_logger):
                thread_logger.info(f"Found this record {request} in Feed Level Suppression Tables. Updating status to 'Z'...")
                updatePostTransactionStatus(thread_logger, table_name, request, 'Z')
                return
            target_id = quota_check(request, thread_logger)
            if target_id is None:
                thread_logger.info(f"Quota limit is reached. Updating status to 'X'...")
                updatePostTransactionStatus(thread_logger, table_name, request, 'X')
                return
            request['targetListId'] = updateTargetListId(thread_logger, table_name, request, target_id)
            if hitTheAPI(thread_logger,request,table_name):
                thread_logger.info(f"Found API hit for record {request} is Success. Updating status to 'C'...")
                updatePostTransactionStatus(thread_logger, table_name, request, 'C')
                return
        except Exception as e:
            print(f"Exception occurred while processing the thread... {request} :: Error reason:: {str(e) + str(traceback.format_exc())}")
            thread_logger.error(f"Exception occurred while processing the thread... {request} :: Error reason:: {str(e) + str(traceback.format_exc())}")
            updatePostTransactionStatus(logger=thread_logger,table_name=table_name,request= request, status='E',ed=str(e))
        finally:
            self.request_queue.task_done()



if __name__ == '__main__':
    obj = Postprocessing()
    obj.post_process()


