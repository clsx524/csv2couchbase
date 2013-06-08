class Reader(object):
    def __init__(self, fp, type):
        pass

    def __iter__(self):
        return self

    def next(self):
        raise NotImplementedError

    def close(self):
        pass

class Writer(object):
    def __init__(self, fp, type):
        pass

    def write(self):
        raise NotImplementedError

    def close(self):
        pass
