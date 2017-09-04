import utils
import logging
import os
import csv
from abc import ABCMeta
from dataservice import AbstractDataService


class AbstractPersistenceService(metaclass=ABCMeta):
    """
    Persistence service pulls data from data service, processes it, and then stores it.
    """
    
    def __init__(self, data_service: AbstractDataService):
        self._logger = logging.getLogger(__name__)
        self._data_service = data_service
        self._results = []
        
    def start(self):
        pass
        
    def stop(self):
        pass
    
    
class CSVEnrollmentPersistenceService(AbstractPersistenceService):
    
    def __init__(self, data_service: AbstractDataService, filename, encoding=None):
        super().__init__(data_service)
        self._filename = filename
        self._encoding = encoding
    
    def output(self):
        rows = utils.OutputHelper.output_rows(self._results, utils.EnrollmentFieldsHelper.get_output_fields(),
                                              utils.EnrollmentFieldsHelper.get_output_field_names())
        try:
            os.makedirs(os.path.dirname(self._filename), exist_ok=True)
        except FileNotFoundError:
            pass
        with open(self._filename, mode='w', encoding=self._encoding) as fle:
            writer = csv.writer(fle)
            writer.writerows(rows)
                
    def start(self):
        # This type of persistence service has no need to continuously get data
        self._logger.debug('CSVPersistenceService started; waiting for data service to finish')
        self._data_service.wait()
        self._logger.debug('wait() returned; data service finished')
        self._results = self._data_service.get_result_list()
        try:
            self.output()
        except Exception:
            self._logger.exception('Error occurred outputting data; returning')
            return
        self._logger.info('Data output done! Output file: %s' % self._filename)
    

class CSVPersistenceService(AbstractPersistenceService):
    
    def __init__(self, data_service: AbstractDataService, filename, encoding=None, rank=True, avg=True):
        super().__init__(data_service)
        self._filename = filename
        self._encoding = encoding
        self._rank = rank
        self._avg = avg
        
    def output(self):
        rows = utils.OutputHelper.output_rows(self._results, utils.FieldsHelper.get_output_fields(), utils.FieldsHelper.get_output_field_names())
        try:
            os.makedirs(os.path.dirname(self._filename), exist_ok=True)
        except FileNotFoundError:
            pass
        with open(self._filename, mode='w', encoding=self._encoding) as fle:
            writer = csv.writer(fle)
            writer.writerows(rows)
    
    def process(self):
        if self._rank:
            for key in utils.FieldsHelper.rankable_fields:
                for major in utils.FieldsHelper.major_list:
                    utils.RankingHelper.rank_column(self._results, key, utils.FieldsHelper.wrap_rank(key),
                                                    range_controller=lambda x: x['major'] == major)
            for key in utils.FieldsHelper.cross_rankable_fields:
                utils.RankingHelper.rank_column(self._results, key, utils.FieldsHelper.wrap_cross_rank(key))
        
        if self._avg:
            for key in utils.FieldsHelper.rankable_fields:
                for major in utils.FieldsHelper.major_list:
                    utils.RankingHelper.average(self._results, key, utils.FieldsHelper.wrap_avg(key),
                                                range_controller=lambda x: x['major'] == major and x[key] != -1)
            for key in utils.FieldsHelper.cross_rankable_fields:
                utils.RankingHelper.average(self._results, key, utils.FieldsHelper.wrap_cross_avg(key), range_controller=lambda x: x[key] != -1)
    
    def start(self):
        # This type of persistence service has no need to continuously get data
        self._logger.debug('CSVPersistenceService started; waiting for data service to finish')
        self._data_service.wait()
        self._logger.debug('wait() returned; data service finished')
        self._results = self._data_service.get_result_list()
        try:
            self.process()
        except Exception:
            self._logger.exception('Error occurred processing data; returning')
            return
        try:
            self.output()
        except Exception:
            self._logger.exception('Error occurred outputting data; returning')
            return
        self._logger.info('Data output done! Output file: %s' % self._filename)
