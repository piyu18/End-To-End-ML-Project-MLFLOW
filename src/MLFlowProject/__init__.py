import os
import sys
import logging
import datetime

logging_str = "[%(asctime)s: %(levelname)s: %(module)s: %(message)s: %(lineno)s]"

log_dir = "logs" 
#filename = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
log_filepath = os.path.join(log_dir,"test.log")
os.makedirs(log_dir,exist_ok=True)

logging.basicConfig(
    level = logging.INFO,
    format = logging_str,
    handlers=[
        logging.FileHandler(log_filepath),
        logging.StreamHandler(sys.stdout),
    ]
)

logger = logging.getLogger("mlprojectlogger")