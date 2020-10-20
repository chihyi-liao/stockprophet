from datetime import timedelta

from stockprophet.cli.common import progressbar
from stockprophet.cli.ta.core import compute
from stockprophet.crawler.utils.date import get_latest_stock_date, get_latest_season_date
from stockprophet.crawler.utils.common import get_stock_dates
from stockprophet.db import get_session
from stockprophet.db import manager as db_mgr
from stockprophet.cli.common import (
    calc_pbr, calc_gross_margin, calc_op_margin, calc_eps,
)


def check_macd_buy_point(macd: list, signal: list, diff: list) -> bool:
    result = False
    if len(diff) < 2:
        return result

    if macd[-1] > 0 and signal[-1] > 0:
        return result

    data = diff[-2:]
    if data[-2] < 0 < data[-1]:
        result = True

    return result


def check_macd_sell_point(macd: list, signal: list, diff: list) -> bool:
    result = False
    if len(diff) < 3:
        return result

    if macd[-1] < 0 and signal[-1] < 0:
        return result

    data = diff[-2:]
    if data[-2] >= 0 >= data[-1]:
        result = True

    return result


def do_get_macd(n_day: int, type_s: str, use_weekly: bool, use_monthly: bool,
                fast: int, slow: int, dif: int, progress: bool = False) -> list:
    result = []

    # 設定初始日期
    date_data = get_stock_dates()
    end_date = get_latest_stock_date(date_data.get("market_holiday", []))
    start_date = end_date - timedelta(days=n_day)
    season_date = get_latest_season_date(end_date)

    s = get_session()
    stock_list = db_mgr.stock.readall_api(s, type_s=type_s, is_alive=True)
    if len(stock_list) == 0:
        return result

    if use_weekly and use_monthly:
        return result

    if use_weekly:
        table = db_mgr.stock_weekly_history
        start_date = start_date - timedelta(days=n_day*7)
    elif use_monthly:
        table = db_mgr.stock_monthly_history
        start_date = start_date - timedelta(days=n_day*30)
    else:
        table = db_mgr.stock_daily_history

    total = len(stock_list)
    for cur, stock in enumerate(stock_list):
        # 處理進度條
        if progress:
            progressbar(cur=cur+1, total=total)

        code = stock['code']
        name = stock['name']
        key = "%s(%s)" % (name, code)
        stock_data = table.read_api(s, code=code, start_date=start_date, end_date=end_date, limit=0)
        values = []
        tmp = dict()
        for i, val in enumerate(stock_data):
            if not val['co']:
                continue

            if i == len(stock_data) - 1:  # 取得最後的值
                data = [code, name, val['co'], val['ch'], int(val['vol'] / 1000)]

                balance_list = db_mgr.stock_balance_sheet.read_api(
                    s, code, start_date=season_date, end_date=season_date, limit=1)

                pbr = calc_pbr(code, val['co'], balance_list=balance_list)
                data.append(pbr if pbr else '')

                income_list = db_mgr.stock_income_statement.read_api(
                    s, code, start_date=season_date, end_date=season_date, limit=1)

                eps = calc_eps(income_list=income_list)
                data.append(eps if eps else '')

                op_margin = calc_op_margin(income_list=income_list)
                data.append(op_margin if op_margin else '')

                gross_margin = calc_gross_margin(income_list=income_list)
                data.append(gross_margin if gross_margin else '')

                tmp[key] = data
            values.append(val['co'])

        if not tmp.get(key):
            continue

        macd, signal, diff = compute.macd(values, fast=fast, slow=slow, n=dif)
        if check_macd_buy_point(macd=macd, signal=signal, diff=diff) is True:
            result.append(tmp[key])

    return result
