class Conf(object):
    inst = None

    def __new__(cls, *args, **kwargs):
        if not cls.inst:
            cls.inst = object.__new__(cls)
        return cls.inst

    def __init__(self):
        self._host = ['localhost:9200']

    def set_host(self, host, port=None, use_ssl=False):
        """
            :param host: localhost / ['localhost:9200', 'other_host:9200']
            :param port: 9200 / none if host is list
            :param use_ssl: if host:port = 443, host on ssl
            :return:
            """
        if isinstance(host, (list, tuple)):
            self._host = [host, use_ssl]
        else:
            self._host = '{host}:{port}'.format(host=host, port=port or 9200)
            print(self.host)

    @property
    def host(self):
        return self._host


Config = Conf()
