from dotenv import load_dotenv
from data_importer import tests_results_importer
import os
import click
import logging


load_dotenv()
connection_string = os.getenv('PY_DWH_CONNECTION_STRING')
log_path = os.getenv('LOGGING_PATH') + 'check_your_skin_loader.log'

FORMAT = '%(asctime)-15s %(name)s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, filename=log_path,level=logging.INFO)
logger = logging.getLogger('urbn.loader.ss')

@cli.command()
def check_your_skin_loader():
    logger.info('Start Check your skin tests results import')
    importer = tests_results_importer()
    importer.connect(connection_string)
    importer.get_tests_results_final()
    importer.save_tests_results()
    importer.disconnect()
    logger.info('End Check your skin tests results import')