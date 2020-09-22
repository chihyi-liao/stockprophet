import logging
import pkg_resources

from configobj import ConfigObj


def read_config() -> ConfigObj:
    config_path = pkg_resources.resource_filename('stockprophet', 'resources/config.ini')
    config = ConfigObj(config_path, encoding='UTF8')
    return config


def read_db_settings() -> dict:
    config_path = pkg_resources.resource_filename('stockprophet', 'resources/config.ini')
    config = ConfigObj(config_path, encoding='UTF8')
    return config.get('database', {})


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    # FileHandler
    file_handler = logging.FileHandler('output.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    # StreamHandler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    # with this pattern, it's rarely necessary to propagate the error up to parent
    logger.propagate = False
    return logger
