from datetime import datetime
import logging
import os

def setup_logger():
    log_file_path = os.path.join(os.path.dirname(__file__), "log.txt")
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
