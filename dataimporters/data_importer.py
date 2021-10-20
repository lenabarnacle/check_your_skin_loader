from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError
from datamodels.datamodel import entity_check_your_skin
from dataimporters.base_importer import base_importer
import constants as const
import pandas as pd
import logging
import base64
import os

import asyncio, aiohttp, datetime, json
from dateutil.rrule import rrule, DAILY

import nest_asyncio
nest_asyncio.apply()

load_dotenv()
domain = os.getenv('DOMAIN')
logger = logging.getLogger('urbn.loader.check_your_skin')


class tests_results_importer(base_importer):


    def fix_and_json(self, s):
        return json.loads(s.replace('\\"', '"'))


    def iter_dates(self, date_from=datetime.date(2021, 10, 4)):
        for dt in rrule(DAILY, dtstart=date_from, until=datetime.date.today()):
            yield dt.strftime("%Y-%m-%d")


    def base64_encode(self, string):
        message_bytes = base64.b64encode(bytes(string, 'utf-8'))
        message = message_bytes.decode('ascii')
        return message


    async def get_tests_results_for_a_date(self, date):
        logger.info('Getting tests results for %s -- start', str(date))

        url = domain + 'wp-content/themes/art&fact/inc/api/csv.php?date=' + str(date)
        auth = aiohttp.BasicAuth(*const.IN_AUTH) if const.IN_AUTH else None
        self.data_final = dict()

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
                data_raw = [x[1:-1] if x.startswith('"') and x.endswith('"') else x for x in data_raw]
                data = dict((k, data_raw[i]) for i, k in enumerate(const.SCHEME_IN))
                self.data_final = self.data_final.append(data)

        logger.info('Getting tests results for %s -- finish', str(date))
        return self.data_final


    def transform_data(self, dict):
        logger.info('Transforming tests results -- start')

        self.df = pd.DataFrame()

        for data in dict:

            data['answers_data'] = self.fix_and_json(data['answers_data'])
            answers_data = pd.DataFrame.from_dict(data['answers_data'], orient='index')[1:]
            if answers_data.empty:
                continue

            data['questions_data'] = self.fix_and_json(data['questions_data'])
            questions_data = pd.DataFrame.from_dict(data['questions_data'], orient='index')[1:]

            test_data = questions_data.merge(answers_data,
                                             how='left',
                                             left_index=True,
                                             right_index=True)
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
                data[results] = self.fix_and_json(data[results])
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
            general_data['data_category'] = 'general'
            general_data['question_num'] = None
            general_data = general_data[['data_category',
                                         'question_num',
                                         'index',
                                         'data']]

            data_out = pd.concat([general_data,
                                  test_data,
                                  results_data], ignore_index=True)
            data_out['test_id'] = self.base64_encode(data['unix_timestamp'] + data['email'])
            data_out = data_out[['test_id',
                                 'data_category',
                                 'question_num',
                                 'index',
                                 'data']]
            self.df = self.df.append(data_out, ignore_index=True)
        logger.info('Transforming tests results -- finish')
        return self.df

    def get_tests_results_final(self):
        df_final = pd.DataFrame()
        for d in self.iter_dates():
            c = asyncio.get_event_loop().run_until_complete(self.get_tests_results_for_a_date(d))
            c_transformed = self.transform_data(c)
            df_final = df_final.append(c_transformed, ignore_index=True)
        self.dict_final = df_final.to_dict(orient='records')
        return self.dict_final

    def save_tests_results(self):
        logger.info('Importing tests results to database -- start')
        try:
            self.session.query(entity_check_your_skin).delete()
            list_of_entity_check_your_skin = [entity_check_your_skin(**e) for e in self.dict_final]
            self.session.add_all(list_of_entity_check_your_skin)
            self.session.commit()
            logger.info('Importing tests results to database -- finish')
        except SQLAlchemyError as err:
            logger.info('SQLAlchemyError', err)
            raise SystemExit(err)
