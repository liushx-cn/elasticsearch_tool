import copy

from ..base_element.elements import BaseDocument, BaseEle, EleFactory, Factory, ClassOperate
from ..base_element.sql_tools import Sql, format_name_to_normal
from ..base_element.exceptions import ParamsError, UsageError


class Doc(BaseDocument, metaclass=ClassOperate):
    """
    文档类，主要的操作类，用于定义文档记录格式，检索文档内容

    class Document(Doc):
        __model__ = MyModel                 model name
        __indices__ = 'my_project'          like database name
        __types__ = 'my_model'              like table name
        __pk__ = 'id'                       your pk field name
        id = Ele(type_=Ele.Integer)         your fields
        text = Ele(type_=Ele.String)
        date = Ele(type_=Ele.Datetime)
        ...
    """
    __model__ = None
    __indices__ = None
    __types__ = None
    __pk__ = 'id'

    def __init__(self, catch_usual=False, *args, **kwargs):

        self.p_c_s = []
        self.catch_usual = catch_usual
        self._docs = {}
        self._p_f_map = {}
        self._had_request = False
        self._use_catch = True
        self._wanted = []
        if not self.__types__:
            self.__types__ = self.__class__.__name__
        self.indices = format_name_to_normal(self.__indices__)
        self.types = format_name_to_normal(self.__types__)

        self._query = Sql(self.indices, self.types)
        self._id = None

        self._limit = None
        self._offset = None

        self._catch = {}
        self._copy = None
        all_c = set(dir(self))^set(dir(object))
        for c in all_c:
            if c in self.__dict__.keys():
                continue
            attr = self.__getattribute__(c)
            if isinstance(attr, BaseEle):
                self.p_c_s.append(c)
        if self.__pk__ not in self.p_c_s:
            raise ParamsError('文档必须有一个主键字段！！Document obj must declare id/pk(name = __pk__)')

    def order_by(self, *args):
        """
        排序方法
        :param args:
        :return:
        """
        body = {'sort': [self.__sort_fields(s) for s in args]}
        self._query.other_params['order'] = body
        return self

    @staticmethod
    def __sort_fields(s):
        if s.startswith('-'):
            return {s[1:]: {'order': 'desc'}}
        else:
            return {s: {'order': 'asc'}}

    def search(self, op=None, **key_words):
        """
        简单搜索,模糊搜索
        .search(doc.field=value)
        :param op: 默认and 可以选择 or, 例如 search(title=value, text=value, op=or)
        若要进行更复杂的匹配, 请使用filter()
        :param key_words: 搜索关键词, 如果进行全文范围匹配, 请使用q=value
        :return:
        """
        option = {'or': 'should'}.get(op, 'must')
        for k, v in key_words.items():
            self._query.match(k, v, op=option)
        return self

    def with_raw(self, body):
        if isinstance(body, dict):
            self._query.raw_search(body)
        else:
            raise TypeError('method with_raw accepted dict')

        return self

    def filter(self, *conditions):
        """
        过滤操作,对文档进行过滤,然后再搜索
        过滤操作的参数是一系列比较运算结果, 默认为 and, 可接受操作为 or NOT > >= < <= != ==
        filter(doc.ele>int,
                doc.ele==value ,
                doc.ele <=value or doc.ele != value,
                NOT(doc.ele=value or doc.ele=value))
        如果需要进行模糊匹配,请配合search()使用
        如:
        Doc().search().filter().all()
        """
        for condition in conditions:
            if isinstance(condition, list):
                self._query.range(None, 'or', condition)
            else:
                self._query.range(*condition.params)
        return self

    def get(self, match=100, op=None, **key_words):
        """
        进行精准简单搜索
        :param match: 精准度,默认100%匹配,如 title=搜索标题, 要求至少三个字匹配,那么 match=75或更高
        :param op: 默认and 可以选择 or, 例如 search(title=value, text=value, op=or)
        若要进行更复杂的匹配, 请使用filter()
        :param key_words: , 如果进行全文范围匹配, 请使用q=value
        :return:
        """
        option = {'or': 'should'}.get(op, 'must')
        for k, v in key_words.items():
            self._query.match_phrase(k, v, op=option, match=match)

        return self

    def limit(self, num):
        """
        分页方法，页容量
        :param num:
        :return:
        """
        self._query.query_params['size'] = num
        self._query.query_params['from_'] = 0
        self._limit = num
        return self

    def offset(self, num):
        """
        分页方法，起始值
        :param num:
        :return:
        """
        if self._limit is None:
            raise UsageError('for paginate, you have to call limit() first')
        self._query.query_params['from_'] = num
        return self

    def count(self):
        """
        计数方法，符合条件的文档数量
        :return:
        """
        return self._query.check_count(self.__pk__)

    def values(self, *args):
        """
        结果筛值
        _source =
        :param args:
        :return:
        """
        for arg in args:
            if arg not in self.p_c_s:
                raise ValueError('no field named %s, choices is %s'%(arg, self.p_c_s))

        self._query.query_params['_source'] = ','.join(args)
        self._wanted.extend(args)
        return self

    def exists(self):
        """
        轨迹方法，符合条件的文档是否存在
        :return: bool
        """
        return self._query.check_exists(self.__pk__)

    def first(self):
        """
        获取第一个
        :return:
        """
        self._query.query_params.update({'size': 1, 'from_': 0})
        result = self.__work()
        return result[0] if result else None

    def all(self):
        """
        获取所有
        :return:
        """
        return self.__work()

    def save(self):
        """
        文档创建保存
        有可能是修改后的保存,有可能是创建
        :return:
        """
        if self._had_request:
            return self._query.put(self, True)
        else:
            return self._query.post(self)

    def query_to_dict(self):
        return {
            key: self.__getattribute__(key).get_value() for key in self.p_c_s
        }

    def get_pk(self):
        if not (self._id is None):
            return self._id
        if self.__getattribute__(self.__pk__) is None:
            return None
        return self.__getattribute__(self.__pk__)

    def __work(self, method='get', use_catch=False):
        """
        执行all()和first()方法时的最终操作
        检索结果,并产出文档类型
        :param method: 暂由内部决定
        :param use_catch: 暂不提供缓存
        :return:
        """
        self._had_request = True
        if self.catch_usual:
            use_catch = self._use_catch or use_catch
        if not use_catch:
            self._docs = self._query.search()
            self._had_request = True
            self._catch[self._query.sql_string] = self._docs
            return self._clone()
        else:
            if self._catch.get(self._query.sql_string, None) is not None:
                return self._catch.get(self._query.sql_string)
            else:
                self._use_catch = False
                return self.__work(method=method, use_catch=False)

    def update(self, **key_words):
        """
        文档更新, 用于更新部分字段, 不管是局部更新还是全部更新,es都会执行删除重建的操作,区别是局部更新节省网络资源
        注意,该方式更新后当前文档实例的数据不会更新为新数据,需要重新获取,如果需要同步更新,请使用先修改,再save()的方式
        :param key_words: field=value,field=value
        :return:
        """
        params = dict(key_words)
        return self._query.put(self, g=False, body={"doc": params})

    def delete(self):
        """
        文档移除
        :return:
        """
        return self._query.delete(self)

    @property
    def ele_fields(self):
        """
        获取检索结果有效字段, 所有的字段, 有可能是列表,有可能是dict
        :return:
        """
        if self._docs.get('hits'):
            return self._docs.get('hits')['hits']
        return self._docs

    def _clone(self):
        """
        返回一个查询结果的拷贝对象
        这里是把查询结果的值相对应的付给文档内的元素对象,然后返回文档的拷贝对象
        这里拷贝的对象是经过分配查询结果的对象,该对象的各个字段值都已经确定
        :return:
        """
        # 拷贝一个当前对象,然后传给处理函数,处理后作为查询结果返回,本实例仍然初始状态,返回的是一个深拷贝的对象

        result = EleFactory(self)
        return result

    def __bool__(self):
        return bool(self.__work())

    def __getitem__(self, item):
        if isinstance(item, slice):
            offset = item.start
            limit = item.stop - offset
        elif isinstance(item, str) and item.isdigit():
            limit = 1
            offset = int(item)
        elif isinstance(item, int):
            limit = 1
            offset = item
        else:
            raise TypeError('What`s wrong with you?! do you know how to use iter[x, y, step]?'
                            '你是傻叉吗?会用切片语法吗?!')
        self.limit(limit)
        self.offset(offset)

        return self


class Fields(object):
    """
    目前暂定文档类型的字段为json字符串,取string类型
    """
    String = Factory('String')
    Integer = Factory('Integer')
    Boolean = Factory('Boolean')
    Datetime = Factory('Datetime')
    List = Factory('List')
    Float = Factory('Float')


"""
可以指定搜索范围为某些索引,所有索引,某些索引下的某些类型
GET: /index1, index2/type1, type2/_search
GET: /a*, b*/c*, d*/_search
"""
