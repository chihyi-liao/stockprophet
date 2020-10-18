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


def do_get_kdj(type_s: str, use_weekly: bool, use_monthly: bool, scalar: int, progress: bool = False) -> list:
    result = []

    # 設定初始日期
    n_day = 9
    date_data = get_stock_dates()
    end_date = get_latest_stock_date(date_data.get("market_holiday", []))
    season_date = get_latest_season_date(end_date)
    start_date = end_date - timedelta(days=scalar*n_day)

    s = get_session()
    stock_list = db_mgr.stock.readall_api(s, type_s=type_s, is_alive=True)
    if len(stock_list) == 0:
        return result

    if use_weekly and use_monthly:
        return result

    if use_weekly:
        table = db_mgr.stock_weekly_history
        start_date = start_date - timedelta(days=(scalar*n_day*7))
    elif use_monthly:
        table = db_mgr.stock_monthly_history
        start_date = start_date - timedelta(days=(scalar*n_day*30))
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
        stock_data = table.read_api(
            s, code=code, start_date=start_date, end_date=end_date, limit=0)
        high_values = []
        low_values = []
        close_values = []
        tmp = dict()
        if len(stock_data) < 9:
            continue

        stock_data = stock_data
        for i, val in enumerate(stock_data):
            if not val['co']:
                continue

            high_values.append(val['hi'])
            low_values.append(val['lo'])
            close_values.append(val['co'])
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
                break

        if not tmp.get(key):
            continue

        k_data, d_data, j_data = compute.kdj(high_values, low_values, close_values, n_day)
        if len(k_data) < 2:
            continue

        if k_data[-1] > 20 or d_data[-1] > 20:
            continue

        # 收集金叉
        if k_data[-1] - k_data[-2] > 0 and k_data[-1] - d_data[-2] > 0:
            result.append(tmp[key])

    return result
