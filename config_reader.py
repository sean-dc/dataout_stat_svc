import configparser
from singleton_meta import Singleton

CFG_PATH = '/etc/siemplus/siemplus.ini'

class Config(metaclass=Singleton):
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(CFG_PATH)
        return

    def getConfig(self):
        return self.config
