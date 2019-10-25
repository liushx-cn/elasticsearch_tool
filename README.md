# ht_es

#### 介绍
python es的再封装

主要借鉴SQLAlchemy的方式

#### 使用说明

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
    for _ in range(100):
        n = random.randint(0, len(words) - 1)
        text += words[n]
    for _ in range(5):
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
        
    DocTry().search(text='检索').all()
    all_re = DocTry().filter(DocTry.num > 50,
                             DocTry.text=='单词',
                             DocTry.text!='一样',
                             OR(DocTry.text == '知识',
                                DocTry.id > 100,
                                DocTry.word.in_(['你们', '那么', '已收到', '一样']),),
                             NOT(DocTry.has_go == False)).all()
    for result in all_re:
        print(result.query_to_dict())
```

更多使用方法,请见: 

#### 参与贡献

联系642641850@qq.com
