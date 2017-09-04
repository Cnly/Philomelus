import io
import logging
import requests
import base64
import utils
from PIL import Image
from abc import ABCMeta
from abc import abstractmethod
from queue import Queue
from threading import Thread
from southocr import southocr


class AbstractCaptchaService(metaclass=ABCMeta):
    
    @abstractmethod
    def _add_captcha_to_pool(self):
        """
        This is the function for the worker threads to try adding captcha results to the pool.
        :return: None
        """
        pass
    
    def __init__(self, worker_count: int, maxsize: int=40):
        self._logger = logging.getLogger(__name__)
        self._worker_count = worker_count
        self._max_pool_size = maxsize
        self._captcha_result_pool = Queue(maxsize)  # Stores captcha results. Concrete structure not defined.
        self._threads = [Thread(target=self._add_captcha_to_pool, name='CaptchaService-worker-%s' % i) for i in range(worker_count)]
        self._running = False
            
    def start(self):
        if not self._running:
            self._logger.debug('CaptchaService starting')
            self._running = True
            for x in self._threads:
                x.start()
            self._logger.debug('CaptchaService started')
    
    @abstractmethod
    def get_captcha_result(self):
        """
        Gets a captcha argument from the pool. Concrete structure not defined. The argument is what needed to be sent
        to the remote when captcha verification is required.
        :return: a captcha argument
        """
        pass
    
    def shutdown(self):
        self._logger.debug('CaptchaService shutting down')
        self._running = False
        self._logger.debug('CaptchaService shut down')
        
        
class NoCaptchaService(AbstractCaptchaService):
    """
    This captcha service is used when there's no captcha verification.
    """
    
    def __init__(self):
        super().__init__(0)
        
    def start(self):
        self._logger.debug('NoCaptchaService started')
        
    def shutdown(self):
        self._logger.debug('NoCaptchaService shut down')

    def get_captcha_result(self):
        return None

    def _add_captcha_to_pool(self):
        pass
    
    
class SouthEnrollmentCaptchaService(AbstractCaptchaService):
    
    def _get_captcha_from_remote(self) -> (Image, dict):
        """
        Gets a new captcha image with its 't' argument from the remote.
        The 't' argument is used when sending enquiry request to the server.
        :return: (img: PIL.Image, t: str)
        """
        response = requests.get('http://service.southcn.com/ksy/?c=vericode&a=simple')
        cookies = response.cookies
        self._logger.debug('Got captcha from remote')
        img_bytes = b''
        for chunk in response.iter_content(chunk_size=2048):
            img_bytes += chunk
        img = Image.open(io.BytesIO(img_bytes))
        
        return img, cookies
    
    @utils.MultithreadingHelper.wrapped
    def _add_captcha_to_pool(self):
        while self._running:
            with self._captcha_result_pool.not_full:
                pass  # We only want to fetch new code when the pool is not full(approx).
            if not self._running:
                return
            self._logger.debug('Preparing to add captcha to pool; current size: %d', self._captcha_result_pool.qsize())
            code = None
            while not code:
                try:
                    img, cookies = self._get_captcha_from_remote()
                    code = southocr.solve(img)
                except Exception:
                    self._logger.exception('Error executing _add_captcha_to_pool')
            self._captcha_result_pool.put((code, cookies))
            self._logger.debug('Adding captcha to pool: %s, %s', code, cookies)
    
    def __init__(self, worker_count: int, maxsize: int = 40):
        super().__init__(worker_count)
    
    def get_captcha_result(self):
        """
        Gets a tuple: (code, t) used by South services
        :return: (code, t)
        """
        return self._captcha_result_pool.get()


class SouthCaptchaService(AbstractCaptchaService):
    
    def _get_captcha_from_remote(self) -> (Image, str):
        """
        Gets a new captcha image with its 't' argument from the remote.
        The 't' argument is used when sending enquiry request to the server.
        :return: (img: PIL.Image, t: str)
        """
        json_body = requests.get('http://94.gaokao.southcn.com/?c=core&a=call&_m=gaokao.code').json()
        self._logger.debug('Got captcha json from remote: %s', json_body)
        t = json_body['args']['t']
        img_bytes = base64.standard_b64decode(json_body['args']['p'])
        img = Image.open(io.BytesIO(img_bytes))
        
        return img, t

    @utils.MultithreadingHelper.wrapped
    def _add_captcha_to_pool(self):
        while self._running:
            with self._captcha_result_pool.not_full:
                pass  # We only want to fetch new code when the pool is not full(approx).
            if not self._running:
                return
            self._logger.debug('Preparing to add captcha to pool; current size: %d', self._captcha_result_pool.qsize())
            code = None
            while not code:
                try:
                    img, t = self._get_captcha_from_remote()
                    code = southocr.solve(img)
                except Exception:
                    self._logger.exception('Error executing _add_captcha_to_pool')
            self._captcha_result_pool.put((code, t))
            self._logger.debug('Adding captcha to pool: %s, %s', code, t)
    
    def __init__(self, worker_count: int, maxsize: int=40):
        super().__init__(worker_count)

    def get_captcha_result(self):
        """
        Gets a tuple: (code, t) used by South services
        :return: (code, t)
        """
        return self._captcha_result_pool.get()
