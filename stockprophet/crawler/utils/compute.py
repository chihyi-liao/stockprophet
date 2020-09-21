from datetime import datetime, date

from sqlalchemy.orm.session import Session

from stockprophet.db import db_lock
from stockprophet.db import manager as db_mgr
from stockprophet.utils import get_logger
from .common import get_stock_dates
from .date import date_range, week_range, check_all_holiday, month_range


logger = get_logger(__name__)


def calc_candlestick(data_list: list) -> dict:
    if len(data_list) == 0:
        return {}

    opening_price, highest_price = None, None
    lowest_price, closing_price = None, None
    trade_volume, trade_value = None, None
    change = 0.0
    for row in data_list:
        op, hi, lo, co = row['op'], row['hi'], row['lo'], row['co']
        vol, val, ch = row['vol'], row['val'], row['ch']
        if op and opening_price is None:
            opening_price = op

        if hi:
            if highest_price is None:
                highest_price = hi
            else:
                if hi > highest_price:
                    highest_price = hi

        if lo:
            if lowest_price is None:
                lowest_price = lo
            else:
                if lo < lowest_price:
                    lowest_price = lo

        if co:
            closing_price = co

        if vol:
            if trade_volume is None:
                trade_volume = vol
            else:
                trade_volume += vol

        if val:
            if trade_value is None:
                trade_value = val
            else:
                trade_value += val

        if ch is not None:
            change += ch
        else:
            change += 0

    return {'op': opening_price, 'hi': highest_price,
            'lo': lowest_price, 'co': closing_price,
            'vol': trade_volume, 'val': trade_value,
            'ch': round(change, 2)}


def calc_weekly_history_table(s: Session, stock_type: str,
                              start_date: date, end_date: date) -> bool:
    logger.info("建立每週收盤行情")
    date_data = get_stock_dates()
    if not date_data:
        logger.error("無法取得休市日期跟補上班日資料")
        return False

    # 取得所有節慶的休市日期(包含颱風假)
    holiday_dates = []
    for str_date in date_data.get("market_holiday", []):
        holiday_dates.append(datetime.strptime(str_date, "%Y-%m-%d").date())

    # 取得補上班日的股市日期
    trading_dates = []
    for str_date in date_data.get("additional_trading_day", []):
        trading_dates.append(datetime.strptime(str_date, "%Y-%m-%d").date())

    # 取得股市類型
    # type_list = db_mgr.stock_type.read_api(s, name=stock_type)
    # if len(type_list) == 0:
    #     logger.error("無法從資料庫取得'<stock_type>'資料")
    #     return False

    stocks = dict()
    # 從資料庫取所有的股票資料
    db_stocks = db_mgr.stock.readall_api(s, type_s=stock_type)
    for r in db_stocks:
        stocks[r['code']] = r['id']

    # 避免start_date給的不是最開始的日期
    start_date, _ = week_range(start_date)
    for current_date in date_range(start_date, end_date):
        wk_start_date, wk_end_date = week_range(current_date)
        # 只處理每週第一天, 避免重複計算
        week_date = wk_start_date
        if current_date != week_date:
            continue

        # 再檢查這一段時間裡面是否皆為放假日
        if check_all_holiday(wk_start_date, wk_end_date, holiday_dates, trading_dates):
            continue

        # 取得 weekly date
        db_lock.acquire()
        wk_date_list = db_mgr.stock_weekly_date.read_api(s, wk_start_date)
        if len(wk_date_list) == 0:
            logger.info("建立 '%s' 的weekly_date", wk_start_date.strftime("%Y-%m-%d"))
            db_mgr.stock_weekly_date.create_api(s, data_list=[{'date': wk_start_date}])
            db_lock.release()
            wk_date_list = db_mgr.stock_weekly_date.read_api(s, wk_start_date)
        else:
            db_lock.release()

        # 取得 stock_date_id
        stock_date_id = wk_date_list[0]['id']

        # 依據每日歷史行情去計算每週的歷史資料表
        logger.info("建立%s '%s' 的weekly_history" % (stock_type, wk_start_date.strftime("%Y-%m-%d")))
        for code, stock_id in stocks.items():
            history_list = db_mgr.stock_daily_history.read_api(s, code, wk_start_date, wk_end_date)
            if len(history_list) == 0:
                continue

            wk_data = calc_candlestick(history_list)
            wk_history_list = db_mgr.stock_weekly_history.read_api(s, code, wk_start_date, wk_end_date)
            if len(wk_history_list) == 0:
                wk_data['stock_id'] = stock_id
                wk_data['stock_date_id'] = stock_date_id
                db_mgr.stock_weekly_history.create_api(s, data_list=[wk_data])
            else:
                oid = wk_history_list[0]['id']
                db_mgr.stock_weekly_history.update_api(s, oid, update_data=wk_data)

        # 更新 metadata
        metadata = db_mgr.stock_metadata.read_api(s)
        if len(metadata) == 1:
            mapping_table = {
                'tse': 'tse_weekly_history_update_date',
                'otc': 'otc_weekly_history_update_date'}
            update_column = mapping_table.get(stock_type)
            if update_column:
                db_mgr.stock_metadata.update_api(
                    s, metadata[0]['id'],
                    update_data={update_column: week_date})
    return True


def calc_monthly_history_table(s: Session, stock_type: str,
                               start_date: date, end_date: date) -> bool:
    logger.info("建立每月收盤行情")
    date_data = get_stock_dates()
    if not date_data:
        logger.error("無法取得休市日期跟補上班日資料")
        return False

    # 取得所有節慶的休市日期(包含颱風假)
    holiday_dates = []
    for str_date in date_data.get("market_holiday", []):
        holiday_dates.append(datetime.strptime(str_date, "%Y-%m-%d").date())

    # 取得補上班日的股市日期
    trading_dates = []
    for str_date in date_data.get("additional_trading_day", []):
        trading_dates.append(datetime.strptime(str_date, "%Y-%m-%d").date())

    # 取得股市類型
    # type_list = db_mgr.stock_type.read_api(s, name=stock_type)
    # if len(type_list) == 0:
    #     logger.error("無法從資料庫取得'<stock_type>'資料")
    #     return False

    stocks = dict()
    # 從資料庫取所有上市的股票資料
    db_stocks = db_mgr.stock.readall_api(s, type_s=stock_type)
    for r in db_stocks:
        stocks[r['code']] = r['id']

    # 避免start_date給的不是最開始的日期
    start_date, _ = month_range(start_date)
    for current_date in date_range(start_date, end_date):
        mn_start_date, mn_end_date = month_range(current_date)

        # 只處理每月第一天, 避免重複計算
        month_date = mn_start_date
        if current_date != month_date:
            continue

        # 再檢查這一段時間裡面是否皆為放假日
        if check_all_holiday(mn_start_date, mn_end_date, holiday_dates, trading_dates):
            continue

        # 取得 monthly date
        db_lock.acquire()
        mn_date_list = db_mgr.stock_monthly_date.read_api(s, mn_start_date)
        if len(mn_date_list) == 0:
            logger.info("建立 '%s' 的monthly_date", current_date.strftime("%Y-%m-%d"))
            db_mgr.stock_monthly_date.create_api(s, data_list=[{'date': mn_start_date}])
            db_lock.release()
            mn_date_list = db_mgr.stock_monthly_date.read_api(s, mn_start_date)
        else:
            db_lock.release()

        # 取得 stock_date_id
        stock_date_id = mn_date_list[0]['id']

        # 依據每日歷史行情去計算每月的歷史資料表
        logger.info("建立%s '%s' 的monthly_history" % (stock_type, mn_start_date.strftime("%Y-%m-%d")))
        for code, stock_id in stocks.items():
            history_list = db_mgr.stock_daily_history.read_api(s, code, mn_start_date, mn_end_date)
            if len(history_list) == 0:
                continue

            mn_data = calc_candlestick(history_list)
            mn_history_list = db_mgr.stock_monthly_history.read_api(s, code, mn_start_date, mn_end_date)
            if len(mn_history_list) == 0:
                mn_data['stock_id'] = stock_id
                mn_data['stock_date_id'] = stock_date_id
                db_mgr.stock_monthly_history.create_api(s, data_list=[mn_data])
            else:
                oid = mn_history_list[0]['id']
                db_mgr.stock_monthly_history.update_api(s, oid, update_data=mn_data)

        # 更新 metadata
        metadata = db_mgr.stock_metadata.read_api(s)
        if len(metadata) == 1:
            mapping_table = {
                'tse': 'tse_monthly_history_update_date',
                'otc': 'otc_monthly_history_update_date'}
            update_column = mapping_table.get(stock_type)
            if update_column:
                db_mgr.stock_metadata.update_api(
                    s, metadata[0]['id'],
                    update_data={update_column: month_date})
    return True
