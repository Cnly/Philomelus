import logging
from collections import OrderedDict


class MultithreadingHelper:
    
    _logger = logging.getLogger(__name__)

    @staticmethod
    def wrapped(func):
        def wrapped_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                MultithreadingHelper._logger.exception('Error executing %s with args %s, %s', func.__name__, args, kwargs)
        return wrapped_func
    
    
class OutputHelper:
    
    _logger = logging.getLogger(__name__)
    
    @staticmethod
    def output_rows(entries: list, output_fields: list, output_field_names: list) -> list:
        expected_result_len = len(output_fields)
        rows = [output_field_names]  # Initialise the first line
        for entry in entries:
            entry_len = len(entry)
            if entry_len != expected_result_len:
                OutputHelper._logger.warning('Wrong length %d(expected %d) in entry: %s', entry_len, expected_result_len, entry)
            row = []
            for key in output_fields:
                row.append(entry.get(key, ''))
            rows.append(row)
        return rows
    
    
class DataStructureHelper:
    
    @staticmethod
    def construct_result_map(sid: str, name: str, ymob: str, major: int, chn: int, mth: int, eng: int, com: int, sum: int):
        return {
            'sid': sid,
            'name': name,
            'ymob': ymob,
            'major': major,  # 0 for arts, 1 for science, -1 for others
            'chn': chn,
            'mth': mth,
            'eng': eng,
            'com': com,
            'sum': sum
        }
    
    @staticmethod
    def construct_enrollment_result_map(sid: str, name: str, ymob: str, type: str, bat: str, uno: str, uname: str):
        return {
            'sid': sid,
            'name': name,
            'ymob': ymob,
            'type': type,
            'bat': bat,
            'uno': uno,
            'uname': uname,
        }
    
    
class EnrollmentFieldsHelper:

    basic_field_names = OrderedDict(
        (
            ('sid', '考号'),
            ('name', '姓名'),
            ('ymob', '出生年月'),
            ('type', '计划类别'),
            ('bat', '批次'),  # batch
            ('uno', '院校代码'),  # university num.
            ('uname', '录取院校')  # university name
        )
    )
    
    basic_field_name_reverse = {
        v: k for k, v in basic_field_names.items()
    }

    @staticmethod
    def get_field_key_by_name(name: str):
        return EnrollmentFieldsHelper.basic_field_name_reverse.get(name)

    @staticmethod
    def get_output_fields():
        # This function's return value should have the same order with get_output_field_names
        return EnrollmentFieldsHelper.basic_field_names.keys()

    @staticmethod
    def get_output_field_names(rank=True, avg=True):
        # This function's return value should have the same order with get_output_fields
        return EnrollmentFieldsHelper.basic_field_names.values()

    @staticmethod
    def get_output_field_name(field):
        return EnrollmentFieldsHelper.basic_field_names[field]
    
    
class FieldsHelper:
    
    # TODO: Support more foreign language types
    
    rankable_fields = (
        'chn',
        'mth',
        'eng',
        'com',
        'sum'
    )
    
    cross_rankable_fields = (
        'chn',
        'eng'
    )
    
    major_list = (
        '文科',
        '理科'
    )
    
    basic_field_names = OrderedDict(
        (
            ('sid', '考号'),
            ('name', '姓名'),
            ('ymob', '出生年月'),
            ('major', '科类'),
            ('chn', '语文'),
            ('mth', '数学'),
            ('eng', '英语'),
            ('com', '综合'),
            ('sum', '总分')
        )
    )
    
    basic_field_name_reverse = {
        v: k for k, v in basic_field_names.items()
    }
    
    rank_suffix = '_rank'
    cross_rank_suffix = '_rank_cross'
    avg_suffix = '_avg'
    cross_avg_suffix = '_avg_cross'
    
    @staticmethod
    def get_field_key_by_name(name: str):
        return FieldsHelper.basic_field_name_reverse.get(name)
    
    @staticmethod
    def get_output_fields(rank=True, avg=True):
        # This function's return value should have the same order with get_output_field_names
        lst = []
        for field, field_name in FieldsHelper.basic_field_names.items():
            lst.append(field)
            if rank or avg:
                if field in FieldsHelper.rankable_fields:
                    if rank:
                        lst.append(FieldsHelper.wrap_rank(field))
                    if avg:
                        lst.append(FieldsHelper.wrap_avg(field))
                if field in FieldsHelper.cross_rankable_fields:
                    if rank:
                        lst.append(FieldsHelper.wrap_cross_rank(field))
                    if avg:
                        lst.append(FieldsHelper.wrap_cross_avg(field))
        return lst
    
    @staticmethod
    def get_output_field_names(rank=True, avg=True):
        # This function's return value should have the same order with get_output_fields
        lst = []
        for field, field_name in FieldsHelper.basic_field_names.items():
            lst.append(field_name)
            if rank or avg:
                if field in FieldsHelper.rankable_fields:
                    if rank:
                        lst.append(FieldsHelper.get_output_field_name(FieldsHelper.wrap_rank(field)))
                    if avg:
                        lst.append(FieldsHelper.get_output_field_name(FieldsHelper.wrap_avg(field)))
                if field in FieldsHelper.cross_rankable_fields:
                    if rank:
                        lst.append(FieldsHelper.get_output_field_name(FieldsHelper.wrap_cross_rank(field)))
                    if avg:
                        lst.append(FieldsHelper.get_output_field_name(FieldsHelper.wrap_cross_avg(field)))
        return lst
    
    @staticmethod
    def wrap_rank(field):
        return field + FieldsHelper.rank_suffix
    
    @staticmethod
    def wrap_cross_rank(field):
        return field + FieldsHelper.cross_rank_suffix
    
    @staticmethod
    def wrap_avg(field):
        return field + FieldsHelper.avg_suffix
    
    @staticmethod
    def wrap_cross_avg(field):
        return field + FieldsHelper.cross_avg_suffix
    
    @staticmethod
    def get_output_field_name(field):
        if field.endswith(FieldsHelper.cross_rank_suffix):
            return FieldsHelper.basic_field_names[field[: - len(FieldsHelper.cross_rank_suffix)]] + '排名'
        elif field.endswith(FieldsHelper.rank_suffix):
            return FieldsHelper.basic_field_names[field[: - len(FieldsHelper.rank_suffix)]] + '科内排名'
        elif field.endswith(FieldsHelper.cross_avg_suffix):
            return FieldsHelper.basic_field_names[field[: - len(FieldsHelper.cross_avg_suffix)]] + '平均'
        elif field.endswith(FieldsHelper.avg_suffix):
            return FieldsHelper.basic_field_names[field[: - len(FieldsHelper.avg_suffix)]] + '科内平均'
        else:
            return FieldsHelper.basic_field_names[field]


class RankingHelper:
    
    # Following functions adapted from Demeter
    
    @staticmethod
    def ranged_results(results, range_controller=None) -> list:
        if not range_controller:
            return results
        return [x for x in results if range_controller(x)]

    @staticmethod
    def rank_result_list(lst: list, key_name: str, result_name: str) -> None:
        try:
            lst = sorted(lst, key=lambda result: result[key_name] if result[key_name] else 0, reverse=True)
        except Exception:
            pass
        previous_rank = 0
        previous_value = 0
        for result, index in zip(lst, range(1, len(lst) + 1)):
            current_value = result[key_name]
            if not previous_rank:  # Initialise the #1
                result[result_name] = 1
                previous_rank = 1
                previous_value = current_value
                continue
            else:
                if current_value == previous_value:
                    # Same value, same rank
                    result[result_name] = previous_rank
                    continue
                else:
                    result[result_name] = index
                    previous_rank = index
                    previous_value = current_value
                    continue

    @staticmethod
    def rank_column(results: list, key_name: str, result_name: str, range_controller=None) -> None:
        ranged_results = RankingHelper.ranged_results(results, range_controller)
        RankingHelper.rank_result_list(ranged_results, key_name, result_name)

    @staticmethod
    def _average0(lst: list, key_name: str, result_name: str, digits = 4) -> None:
        if len(lst) == 0:
            average = 0
        else:
            format = '%.{0}f'.format(digits)
            average = float(format % (float(sum([x[key_name] if x[key_name] else 0 for x in lst])) / float(len(lst))))
        for result in lst:
            result[result_name] = average

    @staticmethod
    def average(results: list, key_name: str, result_name: str, range_controller=None, digits=4) -> None:
        ranged_results = RankingHelper.ranged_results(results, range_controller=range_controller)
        RankingHelper._average0(ranged_results, key_name, result_name, digits=digits)
