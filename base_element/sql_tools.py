"""
此处主要是根据elasticsearch的文档整理的检索关键词,用于检索语句的生成,包括所有相关的工具
"""
import json

from ..base_element.exceptions import UsageError
from .operate import HtEsOperator
from elasticsearch import Elasticsearch as ES
from ..config import Config


EMPTY = (None, '', b'', [], (), {})


def format_name_to_normal(name):
    """将驼峰式命名改成标准小写名 BigName => big_name"""
    if not isinstance(name, str):
        raise TypeError
    letters = list(name)
    index0 = letters[0]
    if index0.isupper():
        letters[0] = index0.lower()

    def to_lower(x):
        if x.isupper():
            return '_' + x.lower()
        return x

    lower = [to_lower(l) for l in letters]
    return ''.join(lower)


class SelectBody(object):

    def __init__(self):
        self.body = {'query': {}}
        self.query = {}
        self._filter = {'filter': []}
        self.q_bool = {'must': [], 'must_not': [], 'should': []}
        self.match = {}
        self._range = {'range': []}

    def init(self):
        self.__init__()

    def treat_values(self, value):
        """
        处理operate类对象,将其转换为普通字典结构数据
        :param value:
        :return:
        """
        if isinstance(value, HtEsOperator):
            v1, op, v2 = value.params
            if op == 'or':
                p1 = self.treat_values(v1)
                p2 = self.treat_values(v2)
                body = [{'should': [p1, p2]}]
            elif op == 'not':
                if isinstance(v2, (list, tuple)):
                    body = [{'must_not': [self.treat_values(v) for v in v2]}]
                else:
                    body = [{'must_not': self.treat_values(v2)}]

            elif op == 'ne':
                if isinstance(v2, (list, tuple)):
                    body = [{'must_not': [{'terms': {v1: v2}}]}]
                else:
                    body = [{'must_not': [{'term': {v1: v2}}]}]
            elif op == 'eq':
                if isinstance(v2, (list, tuple)):
                    body = [{'terms': {v1: v2}}]
                else:
                    body = [{'term': {v1: v2}}]
            elif op == 'in':
                body = [{'terms': {v1: v2}}]
            else:
                body = [{'range': {v1: {op: v2}}}]

        elif isinstance(value, (tuple, list)):  # must
            body = {'must': []}
            for v in value:
                body['must'].append(self.treat_values(v))
        else:
            raise TypeError('value must be instance of HtEsOperator or list of HtEsOperator params')

        return body

    def filter(self, field, op, value):
        """

        :param field:
        :param op:
        :param value:
        :return:
        """
        if op not in ('or', 'not', 'ne', 'eq', 'in'):
            raise ValueError('argument op must be one of or, not, ne, eq, in')

        if op == 'in':
            if self._filter.get('terms'):
                new_terms = [{'terms': {field: value}}, self._filter.pop('terms')]
                new_terms.extend(self.q_bool.pop('must', []))
                self.q_bool['must'] = new_terms

            elif self.q_bool.get('must'):
                terms = [{'terms': {field: value}}, ]
                terms.extend(self.q_bool.pop('must'))
                self.q_bool['must'] = terms

            else:
                self.q_bool['must'] = [{'terms': {field: value}}]

        elif op == 'not':
            terms = []
            for condition in value:
                assert condition.params[1] != 'ne', UsageError('if you want filter not(ele!=value), you can use ele==value instead !')
                terms.extend(self.treat_values(condition))

            terms.extend(self.q_bool.pop('must_not', []))
            self.q_bool['must_not'] = terms

        elif op == 'ne':
            terms = [{'term': {field: value}}, ]
            terms.extend(self.q_bool.pop('must_not', []))
            self.q_bool['must_not'] = terms

        elif op == 'eq':
            terms = [{'match_phrase': {field: value}}, ]
            terms.extend(self.q_bool.pop('must', []))
            self.q_bool['must'] = terms

        else:
            terms = []
            for condition in value:
                terms.extend(self.treat_values(condition))
            terms.extend(self.q_bool.pop('should', []))
            self.q_bool['should'] = terms

    def range(self, field, op, v):
        """

        :param field:
        :param op:
        :param v:
        :return:
        """
        if self._range['range']:
            done = False
            for d in self._range['range']:
                if d.get(field):
                    self._range['range'][field].update({op: v})
                    done = True
            if not done:
                self._range['range'].append({field: {op: v}})
        else:
            self._range['range'].append({field: {op: v}})

    def create(self):
        if self._range['range']:
            for rg in self._range['range']:
                self.q_bool['must'].append({'range': rg})

        if self._filter['filter']:
            self._filter['filter'].extend(self.q_bool.pop('filter', []))
            self.q_bool['filter'] = self._filter['filter']

        if self.q_bool:
            self.query['bool'] = self.q_bool

        self.body['query'] = self.query

        return self


class Query(object):
    """
    主要检索类，用于执行检索，以及文档的增删改查与es服务器的对接
    """

    def __init__(self):
        self.__es = None

    def __get(self, indices=None, types=None, id=None, body=None, _source=None, **kwargs):
        """
        主要的检索方法,包括任何类型的需求
        :param indices:
        :param types:
        :param id:
        :param body:
        :param params:
        :return:
        """
        if id in EMPTY:
            if _source:
                kwargs['_source'] = _source
            return self.__es.search(index=indices, doc_type=types, body=body, **kwargs)
        else:
            if isinstance(id, (list, tuple)):
                return self.__es.mget(body=body)
            else:
                if _source:
                    kwargs['_source'] = _source
                return self.__es.get(index=indices, doc_type=types, id=id, **kwargs)

    def __put(self, indices=None, types=None, id=None, body=None, params=None, g=False):
        """
        修改文档, 修改的文档必须是以确定的,不然修改还有啥意义,那叫创建
        关于更新,官方有两种,局部更新和完全更新,es的更新操作无一例外都是检索出来,删除,新建,局部更新和全部更新都一样
        考虑到文档检索有可能获取全部数据,有可能仅获取是否存在,返回一个id,那么这里也需要做区别处理
        :param indices: 索引值
        :param types: 文档类型
        :param id: 文档id
        :param body: 文档内容(如果为空,则只创建索引)
        :param params: 其他参数
        :param g: 是否全部更新
        :return:
        """
        if id in EMPTY:
            raise ValueError('id must be not empty value!')
        if g:
            return self.__es.index(index=indices, doc_type=types, id=id, body=body, params=params)
        else:
            return self.__es.update(index=indices, doc_type=types, id=id, body=body, params=params)

    def __post(self, indices=None, types=None, id=None, body=None, params=None):
        """
        创建性质的方法,包括创建索引,创建文档对象记录等
        根据官方文档的说法,
        如果文档自带有效Id,那么使用
        PUT: /index/type/id
        否则:
        POST: /index/type  如此,es引擎会自建id
        如果希望创建一个给定了id的文档,那么就需要判断该索引类型id下的文档是否存在,如果存在,那么就失败了.
        文档给出的方式是:
        PUT: /index/type/id/_create or PUT: /index/type/id?op_type=create
        * 官方给的创建方式有几种情况:
            不存在Id, 那么使用 POST
            存在id, 使用PUT :/index/type/id/_create(?op_type=create)
            关于这个操作可以这样理解,存在id的时候,PUT方法是做的修改请求,如果不加参数,那么就回去修改,而不是创建,这样就曲解了我们的意图了.
        It's better to give a id, some as your data's primary key
        :param indices: 索引值
        :param types: 文档类型
        :param id: 文档id
        :param body: 文档内容(如果为空,则只创建索引)
        :param params: 其他参数
        :return:
        """
        if not body:
            # 仅创建索引
            return self.__es.indices.create(index=indices)
        if id in EMPTY:
            # 创建文档索引
            return self.__es.index(index=indices, doc_type=types, id=id, body=body, params=params)
        # id不为空,那么创建文档,并检验该id是否可创建
        return self.__es.create(index=indices, doc_type=types, id=id, body=body, params=params)

    def __delete(self, indices=None, types=None, id=None):
        return self.__es.delete(index=indices, doc_type=types, id=id)

    def __make_es(self):
        if isinstance(Config.host, str):
            return ES([Config.host])
        elif isinstance(Config.host, (list, tuple)):
            return ES(*Config.host)
        else:
            raise TypeError("Query.HOST must be a str like '127.0.0.1:9200' or a list/tuple contains host str,"
                            "but Your HOST is type %s" % type(Config.host))

    def __call__(self, method='get', *args, **kwargs):
        if not self.__es:
            self.__es = self.__make_es()
        methods = {
            'get': self.__get,
            'put': self.__put,
            'del': self.__delete,
            'post': self.__post
        }
        if method not in methods.keys():
            raise ValueError('method argument must be one of [get, put, post, delete], not %s' % method)
        resp = methods[method](*args, **kwargs)
        return resp


class Sql(object):
    """
    查询语句对象类，主要用于生成elastic search可用的查询语句
    """
    __query = Query()

    def __init__(self, indices, types):
        self._stb = SelectBody()
        self.__sql_string = ''
        self.indices = indices
        self.types = types
        self.body = {}
        self.query_params = {}
        self.other_params = {}
        self.with_raw = {}

    def __add__(self, other):
        """
        SQL对象的加法，文档有多个匹配条件时，可以对检索语句做加法处理，整理成一个符合
        格式的SQL结构
        :param other:
        :return:
        """
        if not isinstance(other, self.__class__):
            raise TypeError('For behavior + , must be {0} and {0}, not {1}'.format(self.__class__.__name__,
                                                                                   other.__class__.__name__))
        return True

    @property
    def sql_string(self):
        if self.with_raw:
            self.body = self.with_raw
            self.with_raw = {}

        else:
            self.body = self._stb.create().body
            self._stb.init()

        if self.other_params.get('order'):
            self.body.update(self.other_params.pop('order'))
        return json.dumps(self.body)

    def some_field(self, *args):
        """
        检索部分字段
        :param args:
        :return:
        """
        self.query_params['_source'] = ','.join(args)

    def check_exists(self, pk):
        """
        检查是否存在
        :param pk: 主键的字段名
        :return:
        """
        return bool(self.check_count(pk))

    def check_count(self, pk):
        """
        检查合规数量
        :return:
        """
        if not isinstance(pk, str):
            raise TypeError("'pk' must be str type")
        query_params = {'from_': 0, 'size': 1, '_source': pk}
        result = self.__query('get', indices=self.indices, types=self.types, id=None, body=self.sql_string,
                              **query_params)
        return result['hits'].get('total', 0)

    def put(self, doc, g=False, body=None):
        """
        更新操作,默认调用此方法则会执行局部更新
        :param doc:
        :param g:
        :param body: {doc: {field: value, field: value}}
        :return:
        """
        if g:
            body = doc.query_to_dict()
        indices, types = doc.indices, doc.types
        return self.__query('put', indices=indices, types=types, id=doc.get_pk(), body=body, g=g)

    def post(self, doc, **fields):
        """
        创建文档索引
        :param doc: 文档对象 Doc instance
        """
        indices, types, body = doc.indices, doc.types, doc.query_to_dict()
        return self.__query('post', indices=indices, types=types, id=doc.get_pk(), body=body)

    def delete(self, doc):
        """
        删除文档
        :param doc: 文档对象 Doc instance
        :return:
        """
        return self.__query('del', indices=self.indices, types=self.types, id=doc.get_pk())

    def range(self, f, op, v):
        if op in ('gt', 'ge', 'lt', 'le'):
            self._stb.range(f, op, v)
        else:
            self._stb.filter(f, op, v)

    @staticmethod
    def hole_search(f, op, v):
        """进行了全文检索"""
        if f == 'q':
            return '_all', op, v
        return f, op, v

    def match(self, f, v, op='must'):
        f, op, v = self.hole_search(f, op, v)
        term = [{'match': {f: v}},]
        if self._stb.q_bool:
            term.extend(self._stb.q_bool.pop(op, []))
            self._stb.q_bool[op] = term
        elif self._stb.match:
            self._stb.q_bool[op] = [term, self._stb.match]
            self._stb.match = {}
        else:
            self._stb.match = term

    def match_phrase(self, f, v, match=100, op='must'):
        f, op, v = self.hole_search(f, op, v)
        term = [{'match_phrase': {f: v}}] if match == 100 else [{'match': {f: {"query": v, "minimum_should_match": str(match) + '%'}}}]
        if self._stb.q_bool:
            term.extend(self._stb.q_bool.pop(op, []))
            self._stb.q_bool[op] = term
        elif self._stb.match:
            self._stb.q_bool[op] = [term, self._stb.match]
            self._stb.match = {}
        else:
            self._stb.match = term

    def raw_search(self, body):
        self.with_raw = body

    def search(self):
        result = self.__query('get', indices=self.indices, types=self.types, id=None, body=self.sql_string,
                              **self.query_params)
        if self.query_params:
            self.query_params = {}

        return result
