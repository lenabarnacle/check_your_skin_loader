# from dotenv import load_dotenv
from dataimporters.data_importer import CheckYourSkinLoader
import logging
import os

# load_dotenv()
# log_path = os.path.join(os.getenv('LOGGING_PATH'), 'check_your_skin_loader.log')
CONNECTION_STRING = os.getenv('PY_DWH_CONNECTION_STRING')

FORMAT = '%(asctime)-15s %(name)s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger('check_your_skin_logger')


def check_your_skin_loader():
    logger.info('Start Check_your_skin_tests_results import')
    with CheckYourSkinLoader() as importer:
        importer.run_loader()
    logger.info('End Check_your_skin_tests_results import')

check_your_skin_loader()