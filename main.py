import base64
import datetime
from hashlib import sha1
import hmac
import html
import logging
from logging import handlers
from multiprocessing import Queue
from multiprocessing.dummy import Pool
import os
import shutil
from threading import Lock
import traceback
from urllib.parse import urlparse
import xml.etree.ElementTree as ElementTree

import requests
from requests.exceptions import ConnectTimeout


class Logger(object):

    def __init__(self, filename=None, level=logging.INFO, when='D', back_count=3,
                 fmt='%(asctime)s - %(levelname)s - %(message)s'):
        self.logger = logging.getLogger(filename)
        self.format_str = logging.Formatter(fmt)
        self.logger.setLevel(level)
        self.logger.propagate = False

        if filename is not None:
            if os.path.exists(filename):
                os.remove(filename)
            th = handlers.TimedRotatingFileHandler(filename=filename, when=when, backupCount=back_count,
                                                   encoding='utf-8')
            th.setFormatter(self.format_str)
            self.logger.addHandler(th)

    def add_log_to_screen(self):
        sh = logging.StreamHandler()
        sh.setFormatter(self.format_str)
        self.logger.addHandler(sh)


class ThreadPool:

    def __init__(self, **kwargs):
        assert 'processes' in kwargs or 'thread_pool' in kwargs
        self.thread_pool = 'processes' in kwargs and Pool(kwargs['processes']) or kwargs['thread_pool']
        self.thread_res_queue = Queue()
        self.thread_number = 0

    def start_thread(self, thread_number, func, args=(), kwds={}):
        success = True
        res = None
        try:
            res = func(*args, **kwds)
        except Exception:
            res = traceback.format_exc()
            success = False
        finally:
            self.thread_res_queue.put((thread_number, success, res))

    def apply_async(self, func, args=(), kwds={}):
        self.thread_pool.apply_async(self.start_thread, (self.thread_number, func, args, kwds))
        self.thread_number += 1
        return self

    def get_threads_result(self):
        threads_result = [0]*self.thread_number
        while self.thread_number:
            thread_number, success, res = self.thread_res_queue.get()
            if success:
                threads_result[thread_number] = res
            else:
                raise RuntimeError(f"op=execute thread {thread_number} | status=ERROR | desc={res}")
            self.thread_number -= 1
        return threads_result

    def new_pool_status(self):
        return ThreadPool(thread_pool=self.thread_pool)


class MingleAgent:
    mingle_internal_error = "<h1>We're sorry but Mingle found a problem it couldn't fix</h1>"
    notification = "Please check error_log file"

    def __init__(self, **kwargs):
        self.host = kwargs['host']
        self.project = kwargs['project']
        self.user_name = kwargs['user_name']
        self.secret_key = kwargs['secret_key']
        self.mingle_resource_path = kwargs['mingle_resource_path']
        self.max_card_number = kwargs['mingle_resource_path']

        self.card_number_queue = Queue()
        self.back_up_thread_pool = ThreadPool(processes=2)
        self.cards_detail_thread_pool = ThreadPool(processes=50)
        self.cards_attachment_thread_pool = ThreadPool(processes=10)
        self.attachment_item_thread_pool = ThreadPool(processes=197)
        self.card_murmurs_thread_pool = ThreadPool(processes=50)

        self.failed_cards_pages = []
        self.failed_attachments = []
        self.failed_murmurs = []
        self.save_file_lock = Lock()
        self.file_logger = Logger(filename='error_logs.txt', level=logging.WARNING)
        self.screen_logger = Logger(filename='backup_details.txt', level=logging.INFO)
        self.screen_logger.add_log_to_screen()

    def save_file(self, file_name, content):
        self.save_file_lock.acquire()
        with open(file_name, 'wb') as f:
            f.write(content)
        self.save_file_lock.release()

    def get_mingle_source_with_retries(self, **kwargs):
        tries = 1
        max_retries = 3
        assert ('full_url' in kwargs or 'url' in kwargs)
        kwargs['full_url'] = f'{self.host}/api/v2/projects/{self.project}/{kwargs["url"]}' if 'url' in kwargs else kwargs['full_url']
        if 'timeout' not in kwargs:
            kwargs['timeout'] = 300
        if 'with_auth' not in kwargs:
            kwargs['with_auth'] = True
        if kwargs['with_auth'] and 'auth' not in kwargs:
            kwargs['auth'] = self.MingleAuth(self.user_name, self.secret_key)
        while True:
            tries += 1
            try:
                response = kwargs['with_auth'] and requests.get(url=kwargs['full_url'], auth=kwargs['auth'], timeout=kwargs['timeout']) or requests.get(url=kwargs['full_url'], timeout=kwargs['timeout'])
            except ConnectTimeout:
                if tries == max_retries:
                    raise RuntimeError(f"op=get_mingle_source_with_retries | status=ERROR | desc=Timeout to get {kwargs['full_url']} after {max_retries} retries")
                continue
            if response.status_code != 200:
                if self.mingle_internal_error in response.text:
                    raise RuntimeError(f"op=get_mingle_source_with_retries | status=ERROR | desc=Mingle internal error happen! Failed to fetch {kwargs['full_url']}")
                if tries == max_retries:
                    raise RuntimeError(f"op=get_mingle_source_with_retries | status=ERROR | desc=Failed to get {kwargs['full_url']} after {max_retries} retries, code: {response.status_code}, msg: {response.text}")
            else:
                return response

    def get_cards_of_nth_page(self, page_number):
        try:
            response = self.get_mingle_source_with_retries(url=f'cards.xml?page={page_number}')
            root = ElementTree.fromstring(response.text)
            min_card_number = 9999999
            max_card_number = -1
            card_list = []
            for card in root.iter('card'):
                for info in card:
                    if info.tag == 'number' and info.attrib == {'type': 'integer'}:
                        card_number = int(info.text)
                        card_list.append(card_number)
                        if card_number < min_card_number:
                            min_card_number = card_number
                        if card_number > max_card_number:
                            max_card_number = card_number
            return response.content, min_card_number, max_card_number, card_list
        except Exception:
            self.failed_cards_pages.append(page_number)
            self.screen_logger.logger.error(f"op=get_cards_of_nth_page:{page_number} | status=Failed | desc={self.notification}")
            self.file_logger.logger.error(f"op=get_cards_of_nth_page:{page_number} | status=Failed | desc={traceback.format_exc()}")
            return None, None, None, None

    def get_all_cards(self):
        try:
            batch_size = 200
            n = 1
            thread_pool = self.cards_detail_thread_pool.new_pool_status()
            while True:
                for _ in range(batch_size):
                    thread_pool.apply_async(self.get_cards_of_nth_page, (n,))
                    n += 1
                min_card_number_set = set()
                for content, min_card_number, max_card_number, card_list in thread_pool.get_threads_result():
                    if (content, min_card_number, max_card_number, card_list) == (None, None, None, None):
                        self.screen_logger.logger.error(f"op=get_all_cards | status=Failed | desc=Stop to retrieve cards. {self.notification}")
                        return
                    if min_card_number in min_card_number_set:
                        self.screen_logger.logger.info(f"op=get_all_cards | status=OK | desc=All cards get retrieved successfully")
                        self.card_number_queue.put(None)
                        return
                    else:
                        self.screen_logger.logger.info(f"op=get_cards | status=OK | desc=Finish get cards of {min_card_number} - {max_card_number}")
                        min_card_number_set.add(min_card_number)
                        for card in card_list:
                            self.card_number_queue.put(card)
                        self.save_file(f'{self.mingle_resource_path}/cards/{min_card_number}-{max_card_number}.xml', content)
        except Exception:
            self.screen_logger.logger.error(f"op=get_all_cards | status=Failed | desc=Unknown Exception happen. {self.notification}")
            self.file_logger.logger.error(f"op=get_all_cards | status=Failed | desc=Unknown Exception: {traceback.format_exc()}")

    def get_attachment(self, card_number, attachment_name, attachment_url):
        try:
            res = self.get_mingle_source_with_retries(full_url=attachment_url, with_auth=False, timeout=600)
            self.save_file(f'{self.mingle_resource_path}/attachments/{card_number}-{attachment_name}', res.content)
            self.screen_logger.logger.info(f"op=get_attachment | status=OK | desc=Finish get_attachment for {card_number}-{attachment_name}")
            return True, attachment_name
        except Exception:
            self.screen_logger.logger.error(f"op=get_attachment | status=Failed | desc=Failed to get_attachment for {card_number}-{attachment_name}. {self.notification}")
            self.file_logger.logger.error(f"op=get_attachment | status=Failed | desc=Failed to get_attachment for {card_number}-{attachment_name}: {traceback.format_exc()}")
            return False, attachment_name

    def get_card_attachments(self, card_number):
        retry_times = 0
        max_try_times = 2
        failed_attachments = set()
        try:
            while True:
                response = self.get_mingle_source_with_retries(url=f'cards/{str(card_number)}/attachments.xml')
                if response.text == '<?xml version="1.0" encoding="UTF-8"?>\n<attachments type="array"/>\n':
                    return
                root = ElementTree.fromstring(response.text)
                thread_pool = self.attachment_item_thread_pool.new_pool_status()
                for attachment in root.iter('attachment'):
                    url = html.unescape(attachment.find('url').text)
                    file_name = html.unescape(attachment.find('file_name').text)
                    if retry_times == 0 or (retry_times <= max_try_times and file_name in failed_attachments):
                        thread_pool.apply_async(self.get_attachment, (card_number, file_name, url))
                failed_attachments = set()
                for success, attachment_name in thread_pool.get_threads_result():
                    if not success:
                        failed_attachments.add(attachment_name)
                        if retry_times == max_try_times:
                            self.failed_attachments.append(f'{card_number}-{attachment_name}')
                if retry_times == max_try_times:
                    self.screen_logger.logger.error(f"op=get_card_attachments | status=Failed | desc=Failed to get all attachments for card {card_number} after {max_try_times} tries. {self.notification}")
                    return
                elif len(failed_attachments) == 0:
                    break
                retry_times += 1
                self.screen_logger.logger.warning(f"op=get_card_attachments | status=Retrying | desc=Retrying {retry_times} for card {card_number}")
            self.screen_logger.logger.info(f"op=get_card_attachments | status=OK | desc=Finish get_card_attachments for {card_number}")
        except Exception:
            self.screen_logger.logger.error(f"op=get_card_attachments | status=Failed | desc=Failed to get_card_attachments for {card_number}. {self.notification}")
            self.file_logger.logger.error(f"op=get_card_attachments | status=Failed | desc=Failed to get_card_attachments for {card_number}: {traceback.format_exc()}")

    def get_attachments_and_murmurs(self):
        try:
            thread_pool = self.cards_attachment_thread_pool.new_pool_status()
            while True:
                card_number = self.card_number_queue.get()
                if card_number is None:
                    break
                thread_pool.apply_async(self.get_card_attachments, (card_number,))
                thread_pool.apply_async(self.get_card_murmurs, (card_number,))
            thread_pool.get_threads_result()
            self.screen_logger.logger.info("op=get_attachments_and_murmurs | status=OK | desc=Finish getting attachments and murmurs")
        except Exception:
            self.screen_logger.logger.error(f"op=get_attachments_and_murmurs | status=Failed | desc={self.notification}")
            self.file_logger.logger.error(f"op=get_attachments_and_murmurs | status=Failed | desc={traceback.format_exc()}")

    def get_card_murmurs(self, card_number):
        try:
            response = self.get_mingle_source_with_retries(url=f'cards/{str(card_number)}/murmurs.xml')
            if response.text == '<?xml version="1.0" encoding="UTF-8"?>\n<murmurs type="array"/>\n':
                return
            self.save_file(f'{self.mingle_resource_path}/murmurs/{card_number}.xml', response.content)
            self.screen_logger.logger.info(f"op=get_card_murmurs | status=OK | desc=Succees in get_card_murmurs for {card_number}")
        except Exception:
            self.failed_murmurs.append(card_number)
            self.screen_logger.logger.error(f"op=get_card_murmurs | status=Failed | desc=Failed to get_card_murmurs for {card_number}. {self.notification}")
            self.file_logger.logger.error(f"op=get_card_murmurs | status=Failed | desc=Failed to get_card_murmurs for {card_number}: {traceback.format_exc()}")

    def show_failed_resource(self):
        if self.failed_cards_pages:
            self.screen_logger.logger.error(f"op=show_failed_cards_pages | desc=Failed to retrieve these pages of cards: {str(set(self.failed_cards_pages))}. {self.notification}")
        if self.failed_attachments:
            self.screen_logger.logger.error(f"op=show_failed_attachments | desc=Failed to retrieve these attachments: {str(set(self.failed_attachments))}. {self.notification}")
        if self.failed_murmurs:
            self.screen_logger.logger.error(f"op=show_failed_murmurs | desc=Failed to retrieve these pages of murmurs: {str(set(self.failed_murmurs))}. {self.notification}")

    def back_up_mingle_resource(self):
        try:
            cards_path = f'{self.mingle_resource_path}/cards'
            cards_attachments = f'{self.mingle_resource_path}/attachments'
            cards_murmurs = f'{self.mingle_resource_path}/murmurs'
            if os.path.exists(self.mingle_resource_path) and os.path.isdir(self.mingle_resource_path):
                shutil.rmtree(self.mingle_resource_path)
            os.makedirs(cards_path)
            os.makedirs(cards_attachments)
            os.makedirs(cards_murmurs)
            self.back_up_thread_pool.new_pool_status().\
                apply_async(self.get_all_cards).\
                apply_async(self.get_attachments_and_murmurs).\
                get_threads_result()
            self.screen_logger.logger.info(f"op=back_up_mingle_resource | status=OK | desc=Finish back_up_mingle_resource")
        except Exception:
            self.file_logger.logger.error(f"op=back_up_mingle_resource | status=Failed | desc={self.notification}")
            self.file_logger.logger.error(f"op=back_up_mingle_resource | status=Failed | desc={traceback.format_exc()}")
        finally:
            self.show_failed_resource()

    class MingleAuth:
        def __init__(self, user_name, secret_key):
            self.user_name = user_name
            self.secret_key = secret_key

        def __call__(self, request):
            try:
                path = urlparse(request.url).path
                query = urlparse(request.url).query
                if query:
                    path += '?' + query
                timestamp = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S UTC')
                if request.body:
                    request.headers['content-type'] = 'application/json'
                    canonical_string = 'application/json,,' + path + ',' + timestamp
                else:
                    canonical_string = ',,' + path + ',' + timestamp
                digest = hmac.new(self.secret_key.encode('utf-8'), canonical_string.encode('utf-8'), sha1).digest()
                auth_string = base64.b64encode(digest).decode()

                request.headers['authorization'] = 'APIAuth ' + self.user_name + ':' + auth_string
                request.headers['date'] = timestamp
                return request
            except Exception:
                raise RuntimeError(f"op=MingleAuth __call__ | status=Failed | desc=Failed for {request.url}: {traceback.format_exc()}")


if __name__ == '__main__':
    kwargs = {"host": "",
              "project": "",
              "user_name": "",
              "secret_key": "",
              "mingle_resource_path": "mingle_resource"}

    MingleAgent(**kwargs).back_up_mingle_resource()
