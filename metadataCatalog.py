import os
import json


class MetadataCatalog(object):
    def __init__(self):
        pass

    def write(self, metadata):
        raise NotImplementedError

    def read(self, hash_):
        raise NotImplementedError