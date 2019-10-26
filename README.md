# elasticsearch_tool

#### 介绍
python es的辅助工具,提供类似ORM的操作方式

主要借鉴SQLAlchemy的方式

#### 使用说明

> 目前项目处于测试阶段,由于不同版本的Python/elasticsearch的接口不同,存在一些差异,目前该项目依赖于Python/elasticsearch5.0.0, 已知最新版本的Python/elasticsearch会出错,主要是接口参数错误
该问题会在后期进行解决

##### 安装
`pip3 install elasticsearch_tool`

简单使用

```python
"""
搜索测试工具,数据填充脚本
"""
import random, time
from elasticsearch_tool import Doc, Fields, NOT, OR
from datetime import datetime

from elasticsearch_tool.config import Config


# 配置es服务器地址信息
Config.set_host('localhost', port='9200')


words = ['单词', '词汇', '检索', '我了', '艾克', '维护费', '没理解', '接是', '咯怕', '那么', '行风', '奶茶店', '全网通', '雨天',
         '末尾', '已收到', '就好撒大家看法', '是你们', '你们', '舞女', '不', '容易', '一样', '是你的', '玩儿一天']


def insert_indices():
    text = ''
    word = []
    id = int(time.time() * 1000)
    num = random.randint(000, 999)
    for _ in range(10):
        n = random.randint(0, len(words) - 1)
        text += words[n]
    for _ in range(3):
        n = random.randint(0, len(words) - 1)
        word.append(words[n])

    return {
        'text': text,
        'word': word,
        'id': id,
        'num': num,
        'date': datetime.today(),
        'has_go': num % 2 == 1,
        'height': time.time()
    }


class DocTry(Doc):
    # 配置文档的索引值,类型值,以及文档id的字段
    __indices__ = 'fifth'
    __types__ = 'docs'
    __pk__ = 'id'

    # 设定文档字段和类型
    text = Fields.String
    word = Fields.List
    id = Fields.Integer
    num = Fields.Integer
    date = Fields.Datetime
    has_go = Fields.Boolean
    height = Fields.Float


if __name__ == '__main__':

    for _ in range(15):
        time.sleep(0.01)
        data = insert_indices()
        doc = DocTry()
        for k, v in data.items():
            doc.__setattr__(k, v)

        top = doc.text > 'str'
        nop = doc.num > 20

        doc.save()
    
    # 检索text字段中包含[检索, 检, 索]的文档,这是es默认的简单搜索
    DocTry().search(text='检索').all()
    
    # 搜索文档中:num>50, text匹配词汇[单词],并且不含有词汇[一样],而且必须满足(text包含词汇[知识], 或者文档id大于100, 
    # 或者word字段的词汇包含['你们', '那么', '已收到', '一样']之一)这些条件之一,且has_go字段不等于True的所有文档
    all_re = DocTry().filter(DocTry.num > 50,
                             DocTry.text=='单词',
                             DocTry.text!='一样',
                             OR(DocTry.text == '知识',
                                DocTry.id > 100,
                                DocTry.word.in_(['你们', '那么', '已收到', '一样']),),
                             NOT(DocTry.has_go == False)).all()
    for result in all_re:
        print(result.query_to_dict())
        
    Query = DocTry()
    
    
    # 全文检索和排序, q=检索词 即在所有字段范围内检索, 注意,get 和search方法仅接受关键字参数,不接受比较运算的结果以及位置形参
    Query.search(q='检索').order_by('num').all()
    Query.get(q='检索').order_by('date').first()
    
    # 仅获取部分字段的值
    result = Query.search(q='单词').filter(DocTry.num>50).values('text', 'id', 'num').all()
    print(result)
    
    # 根据id检索
    doc_id = Query.get(id=12345678).first()
    
    # 更新, 推荐该方式
    doc_id.num = 50
    doc_id.save()
    
    # 局部更新方式, 该方式不会改变当前对象, 其区别主要是节省网络资源
    doc_id.update(text='新的文档内容')
    
    # 删除
    doc_id.delete()
    
    # 判断是否存在及数量
    Query.get(q='单词').exists()      # True/False
    Query.get(q='词汇').count()       # int
    
    # 分页, 分页不能单独调用offset(), 且调用顺序必须为 .limit().offset()
    Query.search(q='').limit(30).offset(0).all()
    
    
```
需要注意的是:
任何查询在执行all()/first()方法之前,都不会进行查询,所有匹配操作都要在这两个方法之前调用(exists()和count()除外,这两个并不是查询而是统计)
全文检索的关键字是 q, 所以建立文档时不要用q作为字段名,相同的还有 _all

如果字段类型为列表,想查询字段包含某个词的文档,可以使用字段方法.in_(), 接受一个列表,元组
NOT和OR不支持嵌套,如果想进行特别复杂的查询,文档提供一个接口
`Doc.with_raw()`
```python
    body = {
        'query': {
            'match': {
                'text': '单词'
            }
        }
    }
    resu = Query.order_by('num').limit(3).with_raw(body).all()
    print([res.query_to_dict() for res in resu])
```
该方法接受一个符合es语法的查询结构,需要为字典格式, 该接口仍可配合分页,存在,数量,排序等非查询接口使用

更多使用方法,请见: 

#### 参与贡献

联系642641850@qq.com
