from sqlalchemy.exc import SQLAlchemyError
from datamodel import entity_check_your_skin
from base_importer import base_importer
import pandas as pd
import logging

import asyncio, aiohttp, datetime, json, csv
from dateutil.rrule import rrule, DAILY

import nest_asyncio
nest_asyncio.apply()

logger = logging.getLogger('urbn.loader.ss')


class tests_results_importer(base_importer):

    def fix_and_json(self, s):
        return json.loads(s.replace('\\"', '"'))

    def iter_dates(self, date_from=datetime.date(2021, 10, 1)):
        for dt in rrule(DAILY, dtstart=date_from, until=datetime.date.today()):
            yield dt.strftime("%Y-%m-%d")

    async def get_tests_results_for_a_date(self, date):
        logger.info('Getting tests results for %s', str(date))
        url = f"https://checkyourskin.carely.group/wp-content/themes/art&fact/inc/api/csv.php?date={date}"
        auth = aiohttp.BasicAuth(*IN_AUTH) if IN_AUTH else None
        df = pd.DataFrame({'test_id': [], 'index': [], 'data': []})
        counter = 0

        async with aiohttp.request('get', url, auth=auth) as r:
            if r.status not in IN_GOOD_STATUS:
                raise ValueError(f'Bad http status {r.status}')
            is_header = True
            async for line in r.content:
                line = line.decode('UTF-8').strip('\n')
                if is_header:
                    scheme_diff = set(SCHEME_IN) ^ set(line.split(IN_SEP))
                    if scheme_diff:
                        raise KeyError(f'Scheme different in keys: {list(scheme_diff)}')
                    is_header = False
                    continue
                data_raw = line.split(IN_SEP)
                data = dict((k, data_raw[i][1:-1] if data_raw[i][0] + data_raw[i][-1] == '""' else data_raw[i])
                            for i, k in enumerate(SCHEME_IN))

                data['answers_data'] = self.fix_and_json(data['answers_data'])
                answers_data = pd.DataFrame.from_dict(data['answers_data'], orient='index')[1:]
                if answers_data.empty:
                    continue

                data['questions_data'] = self.fix_and_json(data['questions_data'])
                questions_data = pd.DataFrame.from_dict(data['questions_data'], orient='index')[1:]

                data['results_data_prod'] = self.fix_and_json(data['results_data_prod'])
                results_data_prod = pd.DataFrame.from_dict(data['results_data_prod'], orient='index')
                for i in results_data_prod.columns:
                    results_data_prod[i] = [[x] for x in results_data_prod[i]]
                results_data_prod['data'] = results_data_prod.sum(axis=1)
                results_data_prod['data'] = [list(filter(None, x)) for x in results_data_prod['data']]
                results_data_prod = results_data_prod.reset_index()
                results_data_prod = results_data_prod[['index', 'data']]

                data_df = pd.DataFrame.from_dict(data, orient='index')
                data_df = data_df.rename(columns={0: 'data'})

                test_data = questions_data.merge(answers_data,
                                                 how='left',
                                                 left_index=True,
                                                 right_index=True)
                test_data = test_data.rename(columns={'0_x': 'index', '0_y': 'data'})
                test_data = pd.concat([data_df[:4].reset_index(),
                                       test_data,
                                       results_data_prod,
                                       data_df[-1:].reset_index()], ignore_index=True)
                test_data['test_id'] = data_df['data']['unix_timestamp']
                test_data = test_data[['test_id', 'index', 'data']]

                df = df.append(test_data, ignore_index=True)
                counter += 1
        logger.info(f'Completed {counter} results for {date}')
        return df

    def get_tests_results_final(self):
        self.df = pd.DataFrame()
        for d in self.iter_dates():
            c = asyncio.get_event_loop().run_until_complete(get_tests_results(d))
            self.df = df.append(c, ignore_index=True)
        return self.df

    def save_tests_results(self):
        logger.info('Importing tests results to database -- start',)
        try:
            list_of_entity_check_your_skin = [entity_check_your_skin(**e) for e in self.df]
            self.session.add_all(list_of_entity_check_your_skin)
            self.session.commit()
            logger.info('Importing tests results to database -- finish')
        except SQLAlchemyError as err:
            logger.info('SQLAlchemyError', err)
            raise SystemExit(err)


