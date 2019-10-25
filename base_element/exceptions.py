

__all__ = ('BaseError',
           'UsageError',
           'AssignError',
           'ParamsError',
           'ServerError',
           'DataOutError')


class BaseError(Exception):

    def __init__(self, msg):
        self.message = msg

    def __str__(self):
        return self.message


class UsageError(BaseError):
    """用法错误"""
    pass


class AssignError(BaseError):
    """赋值定义错误"""
    pass


class ParamsError(BaseError):
    """参数错误"""
    pass


class ServerError(BaseError):
    """服务错误"""
    pass


class DataOutError(BaseError):
    """数据错误"""
    pass
