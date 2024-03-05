import json
import requests
import time
from datetime import datetime, timedelta
from loguru import logger
import queue
import psycopg2
from psycopg2 import IntegrityError
from fake_useragent import UserAgent

ID_ROLES_LIST = ['118', '114', '164', '126', '112', '10', '25', '38', '171', '84', '104', '172', '96', '166', '125',
                 '170', '87', '116', '55', '86', '113', '107', '48', '160', '1', '165', '12', '121', '36', '69', '124',
                 '2', '8', '37', '26', '155', '156', '68', '3', '163', '117', '157', '49', '150', '73', '80', '34',
                 '128', '53', '148', '135']

DEFAULT_MAX_REC_RETURNED = 2000
URL = 'https://api.hh.ru/vacancies'
DEFAULT_MAX_STEP_SIZE = 60 * 30
DEFAULT_MIN_STEP_SIZE = 300


class Worker:
    queue_a = queue.Queue()
    queue_b = queue.Queue()
    ua = UserAgent()

    def __init__(self, date_last, date_to):
        self.date_last = date_last
        self.date_to = date_to
        self.ids_set = set()
        self.count = 0
        self.count_errors = 0
        self.headers = {"User-Agent": self.ua.random}

    def api_req(self, page, date_from, date_to, retry=2):
        params = {
            'per_page': 100,
            'page': page,
            'professional_role': ID_ROLES_LIST,
            'date_from': f'{date_from.isoformat()}',
            'date_to': f'{date_to.isoformat()}'}
        req = None
        try:
            req = requests.get(URL, params, headers=self.headers, timeout=5)
            req.raise_for_status()

        except requests.exceptions.ConnectionError as e:
            self.count_errors += 1
            if self.count_errors > 5:
                self.count_errors = 0
                return None
            time.sleep(15)

            return self.api_req(page, date_from, date_to)
        except Exception as err:
            self.count_errors += 1
            if self.count_errors > 5:
                self.count_errors = 0
                return None
            time.sleep(4)
            if retry:

                return self.api_req(page, date_from, date_to, retry=(retry - 1))
            else:
                return self.api_req(page, date_from, date_to)
        else:
            self.count_errors = 0
            self.count += 1
            data = req.content.decode()
            data = json.loads(data)
            if retry:
                if data['items'] == []:
                    data = self.api_req(page, date_from, date_to, retry=(retry - 1))
            else:
                return None
            return data
        finally:
            if req != None:
                req.close()
                if self.count >= 10:
                    time.sleep(1)
                    self.count = 0

    def get_time_step(self, date_left, date_right):
        if date_left < 0:
            date_left = 0
        data = self.api_req(0, self.convert_seconds_in_date(date_left), self.convert_seconds_in_date(date_right))
        if data == None:
            return date_left
        if data['found'] < DEFAULT_MAX_REC_RETURNED:
            self.queue_a.put(
                [self.convert_seconds_in_date(date_left), self.convert_seconds_in_date(date_right)])
        else:
            while date_right != date_left:
                self.queue_a.put([self.convert_seconds_in_date(date_right - DEFAULT_MIN_STEP_SIZE),
                                  self.convert_seconds_in_date(date_right)])
                date_right -= DEFAULT_MIN_STEP_SIZE
        return date_left

    def convert_date_in_seconds(self, date):
        return (date - self.date_last).total_seconds()

    def convert_seconds_in_date(self, seconds):
        return self.date_last + timedelta(days=seconds / (24 * 60 * 60))

    def add_ids_in_set(self, data):
        for i in data['items']:
            self.ids_set.add(i['id'])

    def make_req_ids(self, id, retry=2):
        url = f'{URL}/{id}'
        req = None
        try:
            req = requests.get(url, headers=self.headers, timeout=5)
            req.raise_for_status()
        except requests.exceptions.ConnectionError as e:
            self.count_errors += 1
            if self.count_errors > 5:
                self.count_errors = 0
                return None
            time.sleep(15)
            return self.make_req_ids(id)
        except Exception as err:
            self.count_errors += 1
            if self.count_errors > 5:
                self.count_errors = 0
                return None

            time.sleep(self.count_errors * 2)
            if retry:
                time.sleep(10)
                return self.make_req_ids(id, retry=(retry - 1))
            else:
                return self.make_req_ids(id)
        else:
            self.count_errors = 0
            self.count += 1
            data = req.content.decode()
            return data
        finally:
            if req != None:
                req.close()
                if self.count % 10 == 0:
                    time.sleep(1)

    def process_data_from_queue(self):
        try:
            conn = psycopg2.connect(database='mydatabase', user='myuser', host='postgres', password='mypassword')
            cur = conn.cursor()

            create_table_query = '''
            CREATE TABLE IF NOT EXISTS vacancies (
                vacancies_id INTEGER PRIMARY KEY,
                data_jsonb JSONB
            );
            '''

            cur.execute(create_table_query)
            conn.commit()

            while not self.queue_b.empty():
                data = self.queue_b.get()

                vacancies_id = json.loads(data)['id']

                sql = '''INSERT INTO vacancies (vacancies_id, data_jsonb) VALUES (%s, %s)'''
                try:
                    cur.execute(sql, (vacancies_id, str(data)))
                    conn.commit()
                except IntegrityError:
                    conn.rollback()

        except Exception as err:
            print(err)
        finally:
            cur.close()
            conn.close()
            logger.info('Закончил добавлять в базу')

    def run(self):
        logger.info(f'Запуск парсера. Временной интервал: От {self.date_to} --> до --> {self.date_last}')
        self.date_to = self.convert_date_in_seconds(self.date_to)
        while self.date_to != 0:
            next_date = self.get_time_step(self.date_to - DEFAULT_MAX_STEP_SIZE, self.date_to)
            self.date_to = next_date

        logger.info('Процесс начал парсить ids')

        while not self.queue_a.empty():
            date_step = self.queue_a.get()
            for page in range(20):
                data = self.api_req(page, date_step[0], date_step[1])
                if data == None:
                    break
                if (data['pages'] - page) <= 1:
                    break
                self.add_ids_in_set(data)

        logger.info('Процесс закончил парсить ids')

        for id in self.ids_set:
            data = self.make_req_ids(id)
            if data == None:
                continue
            self.queue_b.put(data)
            if self.count == 500:
                self.process_data_from_queue()
                self.count = 0
        self.process_data_from_queue()
        logger.info('Процесс закончил свою работу')
