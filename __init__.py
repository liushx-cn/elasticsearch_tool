"""
author: Haiton https://www.liushx.com/ 642641850@qq.com
thanks to https://es.xiaoleilu.com/
"""
VERSION = (0, 9, 7)
__version__ = VERSION
__versionstr__ = '.'.join(map(str, VERSION))


from .elements.document import Doc, Fields
from .base_element.operate import NOT, OR, datetime_tool
