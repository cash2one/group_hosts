# -*-coding:utf8-*-
import logging
import json
import sys


class Logger:
    def __init__(self, log_name, file_name):
        self.logger = logging.getLogger(log_name)
        self.logger.setLevel(logging.INFO)

        fh = logging.FileHandler(file_name)
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s -[line:%(lineno)d]%(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def get_logger(self):
        return self.logger


log = Logger('grouphosts', './grouphosts.log').get_logger()


def parse_config(file_name):
    '''
    conparse the config file,just too simple,need to improve
    :param file_name:
    :return:
    '''

    try:
        f = file(file_name)
        obj = json.load(f)
    except Exception, e:
        log.error('read json file error:%s' % e)
        return None
    else:
        return obj


config = parse_config("cfg.json")
if not config:
    sys.exit()

machine_db = config.get('machine_db')
portal_db = config.get('portal_db')
creator = config.get('creator')
transfer = config.get('transfer')

if not machine_db or not portal_db or not transfer:
    log.error("config file error,check it")
    sys.exit()


