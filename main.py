from dotenv import load_dotenv
from dataimporters.data_importer import check_your_skin_loader
import logging
import click
import os

load_dotenv()
connection_string = os.getenv('PY_DWH_CONNECTION_STRING')
log_path = os.path.join(os.getenv('LOGGING_PATH'), 'check_your_skin_loader.log')

FORMAT = '%(asctime)-15s %(name)s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, filename=log_path, level=logging.INFO)
logger = logging.getLogger('urbn.loader.check_your_skin')


@click.command()
def check_your_skin_loader():
    logger.info('Start Check_your_skin_tests_results import')
    importer = check_your_skin_loader()
    importer.connect(connection_string)
    importer.run_loader()
    importer.disconnect()
    logger.info('End Check_your_skin_tests_results import')


if __name__ == '__main__':
    check_your_skin_loader()