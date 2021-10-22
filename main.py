from dotenv import load_dotenv
from dataimporters.data_importer import tests_results_importer
import logging
import click
import os

load_dotenv()
connection_string = os.getenv('PY_DWH_CONNECTION_STRING')
log_path = f"{os.getenv('LOGGING_PATH')}{'check_your_skin_loader.log'}"

FORMAT = '%(asctime)-15s %(name)s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, filename=log_path, level=logging.INFO)
logger = logging.getLogger('urbn.loader.check_your_skin')


@click.group()
def cli():
    pass


@cli.command()
def check_your_skin_loader():
    logger.info('Start Check_your_skin_tests_results import')
    importer = tests_results_importer()
    importer.connect(connection_string)
    importer.check_your_skin_loader_call()
    importer.disconnect()
    logger.info('End Check_your_skin_tests_results import')


if __name__ == '__main__':
    cli()