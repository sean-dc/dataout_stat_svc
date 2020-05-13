import sys
import os
import logging  
from logging.handlers import TimedRotatingFileHandler
from restapi_server import start_rest_api_server

def init_logger():
    if os.path.exists('/var/log/'):
        logging_path = '/var/log/dataout_stat_svc.log'
    else:
        logging_path = 'dataout_stat_svc.log'

    logger = logging.getLogger("")    
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s %(message)s')    

    # add the handler to the root logger
    rotate_handler = TimedRotatingFileHandler(logging_path, when="d", interval=1, backupCount=15)
    rotate_handler.setFormatter(formatter)
    logger.addHandler(rotate_handler)
    logging.info('logger initialized')

if __name__ == '__main__':                        
    init_logger()

    if len(sys.argv) < 2:
        print("usage: python3 {} <PORT>".format(sys.argv[0]))
        sys.exit(1)
    else:
        print("REST PORT : {}".format(sys.argv[1]))

    try:
        #keep_alive()
        rest_port = sys.argv[1]
        start_rest_api_server(rest_port)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()