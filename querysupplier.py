import logging
import csv
import utils
from abc import abstractmethod
from abc import ABCMeta


class AbstractQuerySupplier(metaclass=ABCMeta):
    
    def __init__(self):
        self._logger = logging.getLogger(__name__)
    
    @abstractmethod
    def get_query_sets(self):
        """
        Gets all query sets.
        :return: An iterable of query sets.
        """
        pass
    
    
class LocalQuerySupplier(AbstractQuerySupplier):
    """
    This class reads local file(s) (only csv supported currently) and extract SID and YMOB from them.
    """
    
    def __init__(self, *files, encoding=None):
        super().__init__()
        self._files = files
        self._encoding = encoding
        self._initiated = False
        self._query_sets = set()
        
    def _parse_csv_file(self, fle):
        # TODO: extract file parser functions
        self._logger.info('Parsing csv file %s', fle.name)
        reader = csv.reader(fle)
        rows = [x for x in reader]
        column_names = rows[0]
        column_names = [utils.FieldsHelper.get_field_key_by_name(x) for x in column_names]
        self._logger.debug('Columns in csv file %s: %s', fle.name, column_names)
        data_rows = rows[1:]
        for line_i, row in enumerate(data_rows):
            line_no = line_i + 1
            sid, ymob = None, None
            for ri in range(len(row)):
                key = column_names[ri]
                if key == 'sid':
                    sid = row[ri]
                elif key == 'ymob':
                    ymob = row[ri].zfill(4)
            if not sid or not ymob:
                self._logger.error('Failed to extract sid or ymob at %s:%d: %s', fle.name, line_no, row)
            else:
                self._query_sets.add(((sid, ymob),))  # Pay attention to query set's structure
                self._logger.info('Successfully extracted: %s, %s', sid, ymob)
        
    def get_query_sets(self):
        if not self._initiated:
            for filename in self._files:
                with open(filename, encoding = self._encoding) as fle:
                    if filename.endswith('.csv'):
                        self._parse_csv_file(fle)
                    else:
                        self._logger.error('Unexpected file type in file %s', filename)
            
        return self._query_sets
    
    
class DefaultBruteForceQuerySupplier(AbstractQuerySupplier):
    """
    This supplier generates query sets for brute forcing students' data.
    """
    
    ymob_list = (
        '9903',
        '9906',
        '9901',
        '9805',
        '9810',
        '9807',
        '9812',
        '9902',
        '9904',
        '9905',
        '9907',
        '9908',
        '9811',
        '9809',
        '9808',
        '9806',
        '9909',
        '9910',
        '9911',
        '9912',
        '9804',
        '9803',
        '9802',
        '9801',
        '0001',
        '0002',
        '0003',
        '0004',
        '0005',
        '0006',
        '0007',
        '0008',
        '0009',
        '0010',
        '0011',
        '0012',
        '9712',
        '9711',
        '9710',
        '9709',
        '9708',
        '9707',
        '9706',
        '9705',
        '9704',
        '9703',
        '9702',
        '9701',
        '0101',
        '0102',
        '0103',
        '0104',
        '0105',
        '0106',
        '0107',
        '0108',
        '0109',
        '0110',
        '0111',
        '0112'
    )
    
    @staticmethod
    def _get_query_set_for(sid: str):
        return ((sid, ymob) for ymob in DefaultBruteForceQuerySupplier.ymob_list)
    
    def __init__(self, start, stop):
        super().__init__()
        self._start = start
        self._stop = stop
        pass

    def get_query_sets(self):
        for i in range(self._start, self._stop):
            yield DefaultBruteForceQuerySupplier._get_query_set_for('%010d' % i)
        pass
