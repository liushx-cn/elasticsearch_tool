"""
字段元素类，基本字段，二级字段
"""
import copy
from datetime import datetime

from ..base_element.exceptions import *
from ..base_element.operate import compare_op, datetime_tool


class BaseEle(object):
    TYPES = {
        'String': str,
        'Integer': int,
        'Float': float,
        'Datetime': datetime,
        'Boolean': bool,
        'List': list
    }

    def __init__(self, x=None, m=0,d=0,hour=0,minute=0,second=0, **kwargs):
        self._field_name = ''
        self._t_name = self.__class__.__name__
        self._type = self.TYPES[self._t_name]
        self._doc = None
        self._value = x
        if self._type == list:
            x = x or ()
            list.__init__(self, x)
        elif self._type == datetime and x:
            self._value = datetime(x, m, d, hour, minute, second)

    def set_name(self, name):
        if hasattr(self, '_field_name') and self._field_name:
            raise AssignError('无法修改字段名。You can not rename field name.')

        self._field_name = name

    def __setattr__(self, key, value):
        if key == '_field_name' and hasattr(self, '_field_name') and self._field_name:
            raise AssignError('无法修改字段名。You can not rename field name.')
        else:
            super(BaseEle, self).__setattr__(key, value)

    def set_value(self, value):
        self._value = value

    def get_value(self):
        return self._value

    def __bool__(self):
        if self._t_name == 'Boolean':
            return self._value
        return True

    def __gt__(self, other):
        return compare_op(self, 'gt', other)

    def __ge__(self, other):
        return compare_op(self, 'ge', other)

    def __lt__(self, other):
        return compare_op(self, 'lt', other)

    def __le__(self, other):
        return compare_op(self, 'le', other)

    def __eq__(self, other):
        return compare_op(self, 'eq', other)

    def __ne__(self, other):
        return compare_op(self, 'ne', other)

    def in_(self, li):
        if isinstance(li, (list, tuple)):
            return compare_op(self, 'in', li)
        else:
            raise TypeError('method in_ just accept argument of list/tuple')

    def __get__(self, instance, owner):
        if issubclass(owner, BaseDocument):
            if instance is not None:
                self._doc = instance
            return self
        raise UsageError('该类型只能用在文档类型中作为字段属性，但是你用在了{0}。'
                         'This Type only used in Doc(BaseDocument), '
                         'But you use it in {0}.'.format(owner.__name__))


String = type('String', (BaseEle, BaseEle.TYPES['String']), {})
Integer = type('Integer', (BaseEle, BaseEle.TYPES['Integer']), {})
Boolean = type('Boolean', (BaseEle,), {})
List = type('List', (BaseEle, BaseEle.TYPES['List']), {})
Float = type('Float', (BaseEle, BaseEle.TYPES['Float']), {})
Datetime = type('Datetime', (BaseEle, BaseEle.TYPES['Datetime']), {})


class ClassOperate(type):
    def __new__(cls, cls_name, cls_super, attrs):
        for k, v in attrs.items():
            if isinstance(v, type) and issubclass(v, BaseEle):
                default = {
                    'String': '',
                    'Integer': 0,
                    'Float': 0,
                    'Datetime': (1970, 1, 1),
                    'Boolean': True,
                    'List': ()
                }
                default_value = default[v.__name__]
                if v.__name__ == 'Datetime':
                    value = v(*default_value)
                else:
                    value = v(default_value)
                value.set_name(k)
                attrs[k] = value

        return type.__new__(cls, cls_name, cls_super, attrs)


class BaseDocument(object):
    """
    文档类型基类
    """

    def __getattribute__(self, item):
        """
        if get Doc.Ele, return Ele.value
        else return normal value
        :param item: attribute of Doc
        :return:
        """
        if item != 'p_c_s' and hasattr(self, 'p_c_s') and item in self.p_c_s:
            field = super(BaseDocument, self).__getattribute__(item)
            return field
        return super(BaseDocument, self).__getattribute__(item)

    def __setattr__(self, key, value):
        """
        if set Doc.Ele to value
        set Doc.Ele.value = value
        else treat as normal
        :param key: Doc.Ele name
        :param value: the value to assign
        :return:
        """
        # 在字段定义的时候改变方案,使用产出的方式,产出一个类,然后这里根据类来创建字段对象!!!
        if key != 'p_c_s' and key in self.p_c_s and hasattr(self, key):
            attr = self.__getattribute__(key)

            if attr._type == datetime:
                if isinstance(value, datetime):
                    value = Datetime(value.year, value.month, value.day, value.hour, value.minute, value.second)
                else:
                    raise TypeError('Datetime field must be instance of datetime')
            else:
                if attr._type == bool:
                    value = True if value else False
                value = eval(attr.__class__.__name__)(value)
            value.set_name(key)

        super(BaseDocument, self).__setattr__(key, value)


def ele_factory(doc):
    """
    查询出的文档结果,将由该函数来处理,并改造成一个Doc对象,查询结果将把格式转为详细数据的上一级
    {
        "_index": "megacorp",
        "_type": "employee",
        "_id": "3",
        "_score": 1,
        "_source": {
            "first_name": "Douglas",
            "last_name": "Fir",
            "age": 35,
            "about": "I like to build cabinets",
            "interests": ["forestry"]
        }
    }
    :return:
    """
    if isinstance(doc.ele_fields, list):
        docs = doc.ele_fields
    elif isinstance(doc.ele_fields, dict):
        docs = [doc.ele_fields, ]
    else:
        raise TypeError

    if doc._wanted:
        return [{k: doc_.get('_source', {}).get(k) for k in doc._wanted} for doc_ in docs]

    def init_field(fields):
        doc_obj = copy.deepcopy(doc)

        fields = fields.get('_source', {})
        for field in doc.p_c_s:
            value = fields.get(field)
            if isinstance(doc.__getattribute__(field), datetime):
                value = datetime_tool(value)
            doc_obj.__setattr__(field, value)
        doc._id = fields.get(doc.__pk__)
        return doc_obj


    result = [init_field(doc_) for doc_ in docs]

    return result


def Factory(name):
    return eval(name)


EleFactory = ele_factory
