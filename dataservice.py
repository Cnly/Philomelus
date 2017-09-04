import requests
import utils
import logging
import concurrent.futures
import re
import ast
import csv
import threading
from abc import abstractmethod
from abc import ABCMeta
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

from collections import Iterable

from captchaservice import SouthCaptchaService
from captchaservice import SouthEnrollmentCaptchaService
from captchaservice import NoCaptchaService


class KnownException(BaseException):
    pass
    
    
class UnknownResponseException(BaseException):
    pass


class AbstractDataService(metaclass=ABCMeta):
    
    def __init__(self, worker_count: int, captcha_service, query_supplier):
        self._logger = logging.getLogger(__name__)
        self._worker_count = worker_count
        self._captcha_service = captcha_service
        self._query_supplier = query_supplier
        self._results = []
        self._futures = []
        self._thread_pool_executor = ThreadPoolExecutor(worker_count)
        self._running = False
        
    @abstractmethod
    def _fetch(self, query_set: Iterable):
        """
        Tries to get the data for an SID and put it into the pool.
        :param query_set: queries to make. A iterable whose items are (sid: str, ymob: str) where ymob is year & month of birth(e.g. 9801)
        :return: None
        """
        raise NotImplementedError
    
    def _add_result(self, result):
        self._results.append(result)
        # TODO: implement callbacks
        
    def add_new_data_callback(self, fn):
        raise NotImplementedError
    
    def remove_new_data_callback(self, fn):
        raise NotImplementedError
    
    def get_result_list(self, copy=True):
        if copy:
            return deepcopy(self._results)
        else:
            return self._results
    
    def start(self):
        if not self._running:
            self._logger.debug('DataService starting')
            self._captcha_service.start()
            self._running = True
            for qs in self._query_supplier.get_query_sets():
                future = self._thread_pool_executor.submit(self._fetch, qs)
                self._futures.append(future)
            self._logger.debug('DataService started')
        else:
            self._logger.warning('Illegal state: DataService already started')
            
    def stop(self, wait=True):
        if self._running:
            self._logger.debug('DataService stopping')
            self._running = False
            self._captcha_service.shutdown()
            self._thread_pool_executor.shutdown(wait=wait)
            if wait:
                concurrent.futures.wait(self._futures)
            self._logger.debug('DataService stopped')
        else:
            self._logger.warning('Illegal state: DataService already stopped')
            
    def wait(self):
        self._logger.debug('wait() called')
        # When a new future is added to the list due to error & retry, we need to wait again.
        done = False
        while not done:
            try:
                concurrent.futures.wait(self._futures)
            except ValueError:
                self._logger.debug("ValueError occurred during waiting; should be a result of future list's refreshment")
                pass
            else:
                done = True
        self._logger.debug('waiting ended')
        
    @property
    def futures(self):
        return self._futures
            
    @property
    def thread_pool_executor(self):
        return self._thread_pool_executor
    
    
class LocalDataService(AbstractDataService):
    """
    This data service reads local file(s) and serves the data in them.
    """
    
    log_entry_regex = re.compile('Successfully added entry: (.*)$')
    number_regex = re.compile('[\d.-]+')
    
    def __init__(self, worker_count: int, *files, encoding=None, entry_filter=None):
        """
        Initiates a LocalDataService.
        :param worker_count: The number of threads to use to read the files.
        :param files: File names of the files to read.
        :param entry_filter: A function that accepts one argument, `entry`, and returns True to allow an entry or False to discard.
        """
        super().__init__(worker_count, NoCaptchaService(), None)
        self._files = files
        self._encoding = encoding
        if not entry_filter:
            self._entry_filter = lambda x: True
        else:
            self._entry_filter = entry_filter
        self._sid_set = set()  # used for duplication removal purpose
        self._sid_set_lock = threading.Lock()
            
    def _fetch(self, query_set: Iterable):
        pass

    @staticmethod
    def _convert_value(value: str) -> object:
        """
        Converts a number value to a float object or an empty string to None. Other strings are not converted.
        :param value: The value to convert
        :return: The converted value
        """
        if value == '':
            return None
        elif LocalDataService.number_regex.match(value):
            return float(value)
        else:
            return value
        
    def _sid_unique(self, sid: str) -> bool:
        with self._sid_set_lock:
            return sid not in self._sid_set
    
    @utils.MultithreadingHelper.wrapped
    def _parse_log_file(self, fle):
        self._logger.info('Parsing log file %s', fle.name)
        expected_result_len = len(utils.FieldsHelper.basic_field_names)
        for line_i, line in enumerate(fle):
            line_no = line_i + 1
            try:
                match = LocalDataService.log_entry_regex.search(line)
                if not match:
                    continue
                dict_str = match.group(1)
                entry_map = ast.literal_eval(dict_str)
                if len(entry_map) != expected_result_len:
                    self._logger.error('Entry with wrong length at %s:%d: %s', fle.name, line_no, dict_str)
                sid = entry_map['sid']
                if not self._sid_unique(sid):
                    self._logger.info('SID %s already added. Discarding %s:%d', sid, fle.name, line_no)
                else:
                    self._sid_set.add(sid)
                    self._add_result(entry_map)
                    self._logger.info('Successfully added entry: %s', entry_map)
            except Exception:
                self._logger.exception('Error occurred parsing log file %s, line %d', fle.name, line_no)

    @utils.MultithreadingHelper.wrapped
    def _parse_csv_file(self, fle):
        self._logger.info('Parsing csv file %s', fle.name)
        expected_result_len = len(utils.FieldsHelper.basic_field_names)
        reader = csv.reader(fle)
        rows = [x for x in reader]
        column_names = rows[0]
        column_names = [utils.FieldsHelper.get_field_key_by_name(x) for x in column_names]
        self._logger.debug('Columns in csv file %s: %s', fle.name, column_names)
        data_rows = rows[1:]
        for line_i, row in enumerate(data_rows):
            line_no = line_i + 1
            unique = False
            result = {}
            for ri in range(len(row)):
                key = column_names[ri]
                if not key:  # We don't read in ranking/average data.
                    continue
                value = row[ri]
                if key == 'sid':
                    sid = value.zfill(10)
                    if not unique:
                        if not self._sid_unique(sid):
                            self._logger.debug('SID %s already added. Discarding %s:%d', sid, fle.name, line_no)
                            break
                        unique = True
                        self._sid_set.add(sid)
                    result[key] = sid
                elif key == 'ymob':
                    result[key] = value.zfill(4)
                else:
                    result[key] = LocalDataService._convert_value(value)
            if unique:
                if len(result) != expected_result_len:
                    self._logger.error('Entry with wrong length at %s:%d: %s', fle.name, line_no, row)
                else:
                    self._add_result(result)
                    self._logger.info('Successfully added entry: %s', result)
    
    @utils.MultithreadingHelper.wrapped
    def _parse_file(self, filename: str):
        with open(filename, encoding=self._encoding) as fle:
            if filename.endswith('.log'):
                    self._parse_log_file(fle)
            elif filename.endswith('.csv'):
                self._parse_csv_file(fle)
            else:
                self._logger.error('Unexpected file extension in file %s' % filename)

    def start(self):
        if not self._running:
            self._logger.debug('DataService starting')
            self._running = True
            for fle in self._files:
                future = self._thread_pool_executor.submit(self._parse_file, fle)
                self._futures.append(future)
            self._logger.debug('DataService started')
        else:
            self._logger.warning('Illegal state: DataService already started')

    def stop(self, wait=True):
        if self._running:
            self._logger.debug('DataService stopping')
            self._running = False
            self._thread_pool_executor.shutdown(wait=wait)
            if wait:
                concurrent.futures.wait(self._futures)
            self._logger.debug('DataService stopped')
        else:
            self._logger.warning('Illegal state: DataService already stopped')

    def wait(self):
        self._logger.debug('wait() called')
        # When a new future is added to the list due to error & retry, we need to wait again.
        done = False
        while not done:
            try:
                concurrent.futures.wait(self._futures)
            except ValueError:
                self._logger.debug(
                    "ValueError occurred during waiting; should be a result of future list's refreshment")
                pass
            else:
                done = True
        self._logger.debug('waiting ended')
        
        
class SouthEnrollmentDataService(AbstractDataService):
    
    class InvalidCaptchaException(KnownException):
        """
        Raised when json return code is -1.
        """
        def __init__(self, correct_code=None):
            self._correct_code = correct_code  # Due to a server bug, we can extract the correct verification code in the responses(fixed since 14 Jul)
        
        @property
        def correct_code(self):
            return self._correct_code
        
    
    class InvalidSIDException(KnownException):
        """
        Raised when json return code is -2.
        """
        pass
    
    class InvalidYMOBException(KnownException):
        """
        Raised when json return code is -3.
        """
    
    def __init__(self, worker_count: int, query_supplier, captcha_service: SouthCaptchaService=None, captcha_worker_count: int=2):
        super().__init__(worker_count,
                         SouthEnrollmentCaptchaService(captcha_worker_count) if not captcha_service else captcha_service,
                         query_supplier)
    
    def _parse_json(self, json_body, sid, ymob):
        if 'args' in json_body:
            args = json_body['args']
            return utils.DataStructureHelper.construct_enrollment_result_map(args['zkzh'], args['xm'], ymob,
                                                                             args['jhlb'], args['pc'], args['yxh'],
                                                                             args['yxmc'])
        else:
            return utils.DataStructureHelper.construct_enrollment_result_map(sid, '暂无', ymob, '暂无', '暂无', '暂无', '暂无')
    
    def _fetch0(self, sid, ymob, code, cookies):
        json_body = None
        try:
            json_body = requests.get(
                'http://service.southcn.com/ksy/?c=core&a=call&_m=gklq.search&zkzh=%s&csrq=%s&code=%s'
                % (sid, ymob, code), cookies=cookies).json()
        except Exception as ex:
            self._logger.error('Error occurred fetching data for %s, %s, %s, %s: %s', sid, ymob, code, cookies, ex)
            raise RuntimeError
        status = json_body['code']
        if status == -3:
            raise SouthEnrollmentDataService.InvalidYMOBException()
        elif status == 1 or status == -4:
            return self._parse_json(json_body, sid, ymob)
        elif status == -1:
            # raise SouthEnrollmentDataService.InvalidCaptchaException(json_body['debug']['vrd'])
            raise SouthEnrollmentDataService.InvalidCaptchaException()
        elif status == -2:
            raise SouthEnrollmentDataService.InvalidSIDException()
        else:
            raise UnknownResponseException(json_body)
    
    @utils.MultithreadingHelper.wrapped
    def _fetch(self, query_set: Iterable):
        if not self._running:
            return
        code, cookies = self._captcha_service.get_captcha_result()
        q_sid = None
        # captcha_extracted = False
        for i, (sid, ymob) in enumerate(query_set):
            if not q_sid:
                q_sid = sid
            skip = False
            captcha_valid = True
            while not skip:
                try:
                    # self._logger.debug('Trying to fetch: %s, %s, %s, %s', sid, ymob, code, cookies)
                    # result_map = self._fetch0(sid, ymob, code, cookies)
                    # self._add_result(result_map)
                    # self._logger.info('Successfully added entry: %s', result_map)
                    # return
                    if not captcha_valid:
                        code, cookies = self._captcha_service.get_captcha_result()
                        captcha_valid = True
                    self._logger.debug('Trying to fetch: %s, %s, %s, %s', sid, ymob, code, cookies)
                    result_map = self._fetch0(sid, ymob, code, cookies)
                    self._add_result(result_map)
                    self._logger.info('Successfully added entry: %s', result_map)
                    return
                except SouthEnrollmentDataService.InvalidYMOBException:
                    self._logger.debug('Invalid YMOB: %s, %s', sid, ymob)
                    skip = True
                except SouthEnrollmentDataService.InvalidSIDException:
                    self._logger.warning('Invalid SID: %s', sid)
                    return
                # except SouthEnrollmentDataService.InvalidCaptchaException as ex:
                    # if not captcha_extracted:
                    #     self._logger.debug('Invalid captcha: %s, %s; extracting from response', code, cookies)
                    #     code = ex.correct_code
                    #     captcha_extracted = True
                    # else:
                    #     self._logger.warning('Still invalid captcha from response! Code & cookies: %s, %s; re-getting from pool', code, cookies)
                    #     code, cookies = self._captcha_service.get_captcha_result()
                    #     captcha_extracted = False
                except SouthEnrollmentDataService.InvalidCaptchaException:
                    self._logger.debug('Invalid captcha: %s, %s', code, cookies)
                    captcha_valid = False
                except RuntimeError:
                    self._logger.info('Re-adding task for %s, %s', sid, ymob)
                    future = self._thread_pool_executor.submit(self._fetch, query_set)
                    self._futures.append(future)
                    return
                except Exception:
                    self._logger.exception('Error fetching or adding data')
        self._logger.warning('No valid YMOB for SID %s', q_sid)


class SouthDataService(AbstractDataService):
    
    class InvalidCaptchaException(KnownException):
        """
        Raised when json return code is -2.
        """
        pass
    
    class InvalidSIDException(KnownException):
        """
        Raised when json return code is -3.
        """
        pass
    
    class InvalidYMOBException(KnownException):
        """
        Raised when json return code is -4.
        """
    
    def __init__(self, worker_count: int, query_supplier, captcha_service: SouthCaptchaService=None, captcha_worker_count: int=2):
        super().__init__(worker_count, SouthCaptchaService(captcha_worker_count) if not captcha_service else captcha_service, query_supplier)
    
    def _parse_json(self, json_body, ymob):
        args = json_body['args']
        subject_list = args['list']
        major = '未知'
        chn = -1
        mth = -1
        eng = -1
        com = -1
        sum = -1
        for submap in subject_list:
            subject = submap['km']
            if subject == '语文':
                chn = int(submap['cj'])
            elif subject.endswith('数学'):
                mth = int(submap['cj'])
            elif subject == '英语(含听说)':
                eng = int(submap['cj'])
            elif subject.endswith('综合'):
                com = int(submap['cj'])
            elif subject.endswith('总分'):
                sum = int(submap['cj'])
                major = subject[:-2]
        if major == '未知':
            self._logger.error('Unable to extract major from json: %s', json_body)
        elif major not in utils.FieldsHelper.major_list:
            self._logger.error('Unexpected major type "%s" in json: %s', major, json_body)
        if eng == -1:
            self._logger.warning('No English field for %s, %s! Json: %s', args['zkzh'], ymob, json_body)
        return utils.DataStructureHelper.construct_result_map(args['zkzh'], args['xm'], ymob, major, chn, mth, eng, com, sum)
    
    def _fetch0(self, sid, ymob, code, t):
        json_body = None
        try:
            json_body = requests.get(
                'http://94.gaokao.southcn.com/?c=core&a=call&_m=gaokao.search&zkzh=%s&csrq=%s&code=%s&t=%s'
                % (sid, ymob, code, t)).json()
        except Exception as ex:
            self._logger.error('Error occurred fetching data for %s, %s, %s, %s: %s', sid, ymob, code, t, ex)
            raise RuntimeError
        status = json_body['code']
        if status == -4:
            raise SouthDataService.InvalidYMOBException()
        elif status == 1:
            return self._parse_json(json_body, ymob)
        elif status == -2:
            raise SouthDataService.InvalidCaptchaException()
        elif status == -3:
            raise SouthDataService.InvalidSIDException()
        else:
            raise UnknownResponseException(json_body)
        
    @utils.MultithreadingHelper.wrapped
    def _fetch(self, query_set: Iterable):
        if not self._running:
            return
        code, t = self._captcha_service.get_captcha_result()
        q_sid = None
        for i, (sid, ymob) in enumerate(query_set):
            if not q_sid:
                q_sid = sid
            skip = False
            captcha_valid = True
            while not skip:
                try:
                    if not captcha_valid:
                        code, t = self._captcha_service.get_captcha_result()
                        captcha_valid = True
                    self._logger.debug('Trying to fetch: %s, %s, %s, %s', sid, ymob, code, t)
                    result_map = self._fetch0(sid, ymob, code, t)
                    self._add_result(result_map)
                    self._logger.info('Successfully added entry: %s', result_map)
                    return
                except SouthDataService.InvalidYMOBException:
                    self._logger.debug('Invalid YMOB: %s, %s', sid, ymob)
                    skip = True
                except SouthDataService.InvalidSIDException:
                    self._logger.warning('Invalid SID: %s', sid)
                    return
                except SouthDataService.InvalidCaptchaException:
                    self._logger.debug('Invalid captcha: %s, %s', code, t)
                    captcha_valid = False
                except RuntimeError:
                    self._logger.info('Re-adding task for %s, %s', sid, ymob)
                    future = self._thread_pool_executor.submit(self._fetch, query_set)
                    self._futures.append(future)
                    return
                except Exception:
                    self._logger.exception('Error fetching or adding data')
        self._logger.warning('No valid YMOB for SID %s', q_sid)
