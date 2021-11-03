from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError
from datamodels.datamodel import entity_check_your_skin
from dataimporters.base_importer import base_importer
import constants as const
import pandas as pd
import logging
import hashlib
import os

import asyncio, aiohttp, datetime, json
from dateutil.rrule import rrule, DAILY

# import nest_asyncio
# nest_asyncio.apply()

load_dotenv()
CONNECTION_STRING = os.getenv('PY_DWH_CONNECTION_STRING')
IN_AUTH = (os.getenv('CHECK_YOUR_SKIN_LOGIN'), os.getenv('CHECK_YOUR_SKIN_PASSWORD'))
DOMAIN = os.getenv('CHECK_YOUR_SKIN_DOMAIN_1')

logger = logging.getLogger('urbn.loader.check_your_skin')


def fix_and_json(s):
    return json.loads(s.replace('\\"', '"'))


def get_hash(string):
    string = string.encode('utf-8')
    return hashlib.sha1(string).hexdigest()


def iter_dates(date_from=datetime.date(2021, 10, 4)):
    for dt in rrule(DAILY, dtstart=date_from, until=datetime.date.today()):
        yield dt.strftime("%Y-%m-%d")


class CheckYourSkinLoader(base_importer):

    def __enter__(self):
        self.connect(CONNECTION_STRING)
        return self

    def __exit__(self, type, value, traceback):
        return self.disconnect()

    async def get_tests_results_for_a_date(self, date):
        logger.info('Getting tests results for %s -- start', str(date))
        url = f"{DOMAIN}{'wp-content/themes/art&fact/inc/api/csv.php?date='}{str(date)}"
        auth = aiohttp.BasicAuth(*IN_AUTH) if IN_AUTH else None
        reports_data = []

        async with aiohttp.request('get', url, auth=auth) as r:

            if r.status not in const.IN_GOOD_STATUS:
                logger.info('Bad http status %s', str(r.status))
                raise ValueError(f'Bad http status {r.status}')

            is_header = True
            async for line in r.content:
                line = line.decode('UTF-8').strip('\n')

                if is_header:
                    scheme_diff = set(const.SCHEME_IN) ^ set(line.split(const.IN_SEP))
                    if scheme_diff:
                        logger.info('Scheme different in keys: %s', str(list(scheme_diff)))
                        raise KeyError(f'Scheme different in keys: {list(scheme_diff)}')
                    is_header = False
                    continue

                data_raw = line.split(const.IN_SEP)
                data = dict()
                for i, k in enumerate(const.SCHEME_IN):
                    if data_raw[i].startswith('"') and data_raw[i].endswith('"'):
                        data[k] = data_raw[i][1:-1]
                    else:
                        data[k] = data_raw[i]
                reports_data.append(data)

        logger.info('Getting tests results for %s -- finish', str(date))
        return reports_data

    def transform_data(self, reports_data):
        logger.info('Transforming tests results -- start')

        df = pd.DataFrame()

        for data in reports_data:

            answers_data = fix_and_json(data['answers_data'])
            answers_data = pd.DataFrame.from_dict(answers_data, orient='index')[1:]
            if answers_data.empty:
                continue

            questions_data = fix_and_json(data['questions_data'])
            questions_data = pd.DataFrame.from_dict(questions_data, orient='index')[1:]

            test_data = questions_data.merge(answers_data,
                                             how='left',
                                             left_index=True,
                                             right_index=True)

            questions_data = None
            answers_data = None

            test_data = test_data.reset_index()
            test_data['data_category'] = 'test'
            test_data = test_data.rename(columns={'index': 'question_num',
                                                  '0_x': 'index',
                                                  '0_y': 'data'})
            test_data = test_data[['data_category',
                                   'question_num',
                                   'index',
                                   'data']]

            results_data = pd.DataFrame()
            for results in ['results_data_prod', 'results_data_test']:
                data[results] = fix_and_json(data[results])
                if not data[results]:
                    continue
                results_part = pd.DataFrame.from_dict(data[results], orient='index')
                if results_part.empty:
                    continue
                for i in results_part.columns:
                    results_part[i] = [[x] for x in results_part[i]]
                results_part['data'] = results_part.sum(axis=1)
                results_part['data'] = [list(filter(None, x)) for x in results_part['data']]
                results_part = results_part.reset_index()
                results_part['data_category'] = results
                results_part['question_num'] = None
                results_part = results_part[['data_category',
                                             'question_num',
                                             'index',
                                             'data']]
                results_data = results_data.append(results_part, ignore_index=True)

            general_data = pd.DataFrame.from_dict(data, orient='index')
            general_data = general_data.rename(columns={0: 'data'})
            general_data = general_data[:4].reset_index()
            general_data = pd.concat([
                pd.DataFrame({'index': ['domain'],'data': [DOMAIN]}),
                general_data], ignore_index=True)
            general_data['data_category'] = 'general'
            general_data['question_num'] = None
            general_data = general_data[['data_category',
                                         'question_num',
                                         'index',
                                         'data']]

            data_out = pd.concat([general_data,
                                  test_data,
                                  results_data], ignore_index=True)
            data_out['test_id'] = get_hash(f"{data['unix_timestamp']}{data['email']}")
            data_out = data_out[['test_id',
                                 'data_category',
                                 'question_num',
                                 'index',
                                 'data']]
            df = df.append(data_out, ignore_index=True)
        logger.info('Transforming tests results -- finish')
        return df

    def get_tests_results_final(self):
        df_final = pd.DataFrame()
        for d in iter_dates():
            reports_data = asyncio.get_event_loop().run_until_complete(self.get_tests_results_for_a_date(d))
            df = self.transform_data(reports_data)
            df_final = df_final.append(df, ignore_index=True)
        dict_final = df_final.to_dict(orient='records')
        return dict_final

    def save_tests_results(self, dict_final):
        logger.info('Importing tests results to database -- start')
        try:
            self.session.query(entity_check_your_skin).delete()
            list_of_entity_check_your_skin = [entity_check_your_skin(**e) for e in dict_final]
            self.session.add_all(list_of_entity_check_your_skin)
            self.session.commit()
            logger.info('Importing tests results to database -- finish')
        except SQLAlchemyError as err:
            logger.info('SQLAlchemyError', err)
            raise SystemExit(err)

    def run_loader(self):
        dict_final = self.get_tests_results_final()
        self.save_tests_results(dict_final)
