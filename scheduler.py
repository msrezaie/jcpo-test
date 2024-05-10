import logging
import requests
from dukeauth import get_auth
from dukeoutages import main


"""
This script is used to schedule the decoutages script
to run every 10 minutes, and log each occurrence of
the task in the terminal.
"""
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

c_handler = logging.StreamHandler()

c_handler.setLevel(logging.INFO)

c_format = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
c_handler.setFormatter(c_format)

logger.addHandler(c_handler)

random_url = "https://outagemap.duke-energy.com/"

r = requests.get(random_url)

if __name__ == '__main__':
    try:
        print(r.status_code)
        logger.info("Outage scripts ran successfully.")
    except ValueError as e:
        print(e)
    except Exception as e:
        print(e)
