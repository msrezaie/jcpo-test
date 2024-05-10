import logging
import schedule
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

headers, cookies = get_auth()


def run_script():
    try:
        main(headers, cookies)
        logger.info("Outage scripts ran successfully.")
    except Exception as e:
        logger.exception(f"Task failed with error: {e}")


run_script()

# Schedule the script to run every 10 minutes
schedule.every(10).minutes.do(run_script)

# Run the scheduled tasks continuously
while True:
    schedule.run_pending()