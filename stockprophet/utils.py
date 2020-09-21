import os
import logging
from pathlib import Path
from configobj import ConfigObj


def get_project_root() -> Path:
    return Path(__file__).parent.parent


def read_config() -> dict:
    root_path = get_project_root()
    config_path = os.path.join(root_path, 'conf', 'config.ini')
    config = ConfigObj(config_path, encoding='UTF8')
    return config


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
