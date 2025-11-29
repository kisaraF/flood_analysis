import logging
from datetime import datetime
import os

def init_logger(log_type:str) -> None:
    log_date = datetime.now().strftime('%Y%m%d%H%M')
    log_fp = f'{os.getcwd()}/logs/{log_type}_log_{log_date}.log'
    logging.basicConfig(
        level=logging.INFO,
        filename=log_fp,
        filemode='w',
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info(f"Log file initiated at {datetime.now()}")