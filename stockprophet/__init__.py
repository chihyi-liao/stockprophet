from stockprophet.cli import entry_point
from stockprophet.crawler import (
    init_stock_type, init_stock_category, init_stock_metadata
)
from stockprophet.db import init_db
from stockprophet.utils import read_config


def preprocessing() -> bool:
    result = False
    # noinspection PyBroadException
    try:
        config = read_config()
        db_config = config.get('database', {})
        if not db_config:
            print("config.ini 找不到 'database' 區段")
            return result
    except Exception:
        print("無法讀取或解析config.ini")
        return result

    # noinspection PyBroadException
    try:
        init_db(db_config)
        init_stock_metadata()
        init_stock_category()
        init_stock_type()
        result = True
    except Exception:
        print("無法建立資料庫")
    return result


def main():
    if preprocessing():
        entry_point()
