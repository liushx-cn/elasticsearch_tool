from operator import gt, lt, le, ge, eq, ne
import re
from datetime import datetime

from ..base_element.exceptions import UsageError

operate_map = {
    'gt': gt,
    'ge': ge,
    'lt': lt,
    'le': le,
    'eq': eq,
    'ne': ne,
    'in': lambda x, y: x in y
}


class HtEsOperator(object):

    def __init__(self, value, op, bo, other):
        self.value = value
        self.op = op
        self.bo = bo
        self.other = other

    @property
    def params(self):
        if self.op in operate_map.keys():
            return self.value._field_name, self.op, self.other
        return self.value, self.op, self.other

    def __bool__(self):
        return self.bo

    def __or__(self, other):
        if isinstance(other, self.__class__):
            return HtEsOperator(self, 'or', True, other)
        else:
            return self


def compare_op(obj, op, other):
    bo = operate_map[op](obj._value, other)
    return HtEsOperator(obj, op, bo, other)


def not_(*x):
    if isinstance(x, HtEsOperator):
        if x.params[1] == 'not':
            raise UsageError('if use such as NOT(NOT(ele==v or ele != v2)), you can do it like ele==v or ele!=v2')
        if not _validate_option(x):
            raise UsageError('function NOT/OR not accept result of NOT/OR')

    elif not isinstance(x, (list, tuple)):
        raise TypeError('function Not_ just accept argument which is instance of HtEsOperator !')
    return HtEsOperator(None, 'not', False, x)


def _validate_option(x):
    if isinstance(x, HtEsOperator):
        if x.params[1] in ('not', 'or'):
            raise UsageError('function NOT/OR not accept result of NOT/OR')
        return True


def or_(*args):
    conditions = [arg for arg in args if _validate_option(arg)]
    return conditions


def datetime_tool(dt=None, fmt=None):
    """
    时间日期转换工具
    datetime and str transformer
    给一个标准格式的时间日期字符串,会输出该日期字符串的datetime对象
    标准格式如: 2019/08/15 12:59:59 or 2019年5月13日 14点30分59秒
    :param dt: datetime str or datetime
    :param fmt: datetime's format default: %Y-%m-%d %H:%M:%S
    :return:
    """

    def to_int(x):
        if x is None:
            return 0
        if x.isdigit():
            return int(x)
        return x

    if dt:
        if isinstance(dt, datetime):
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(dt, str):
            result = re.match(r'(\d+)\D(\d+)\D(\d+)(\D+?(\d+)[^0-9]*(\d+)?[^0-9]*(\d+)?[^0-9]*)?', dt)
            if not result:
                raise ValueError('date or datetime must be standard format such as '
                                 'year-month-day or year-month-day hour:minute:second')
            Y, m, d, has_time, H, M, S = map(to_int, result.groups())
            return datetime(Y, m, d, H, M, S)
        else:
            raise TypeError('function datetime_tool accepted one argument must be '
                            'instances of datetime or datetime style str')
    else:
        if fmt:
            return datetime.today().strftime(fmt)
        else:
            return datetime.today().strftime('%Y-%m-%d %H:%M:%S')


NOT = not_
OR = or_