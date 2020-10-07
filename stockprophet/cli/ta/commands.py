from datetime import timedelta
import click

from stockprophet.cli.ta import compute
from stockprophet.crawler.utils.date import get_latest_stock_date, get_latest_season_date
from stockprophet.crawler.utils.common import get_stock_dates
from stockprophet.db import get_session
from stockprophet.db import manager as db_mgr
from stockprophet.cli.common import (
    show_result, calc_pbr, calc_gross_margin, calc_op_margin, calc_eps,
)


@click.group()
def ta_group():
    pass


# --- get --- #
@ta_group.group('get')
def get_group():
    pass


@get_group.command('macd')
@click.option('--n_day', '-n', default=60, show_default=True,
              type=click.IntRange(1, 90), help="資料表計算天數")
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--use_weekly', is_flag=True, help='使用每週歷史資料表來計算')
@click.option('--use_monthly', is_flag=True, help='使用每月歷史資料表來計算')
@click.option('--fast', '-f', default=12, show_default=True, type=int, help='macd 快線參數')
@click.option('--slow', '-s', default=26, show_default=True, type=int, help='macd 慢線參數')
@click.option('--dif', '-d', default=9, show_default=True, type=int, help='macd 差離值參數')
def get_macd(n_day, type_s, use_weekly, use_monthly, fast, slow, dif):
    # 設定初始日期
    date_data = get_stock_dates()
    end_date = get_latest_stock_date(date_data.get("market_holiday", []))
    start_date = end_date - timedelta(days=n_day)
    season_date = get_latest_season_date(end_date)

    s = get_session()
    stock_list = db_mgr.stock.readall_api(s, type_s=type_s, is_alive=True)
    if len(stock_list) == 0:
        return

    if use_weekly and use_monthly:
        click.echo("無法同時指定兩個歷史資料表")
        return

    if use_weekly:
        table = db_mgr.stock_weekly_history
        start_date = start_date - timedelta(days=n_day*7-n_day)
    elif use_monthly:
        table = db_mgr.stock_monthly_history
        start_date = start_date - timedelta(days=n_day*30-n_day)
    else:
        table = db_mgr.stock_daily_history

    result = []
    for stock in stock_list:
        code = stock['code']
        name = stock['name']
        key = "%s(%s)" % (name, code)
        stock_data = table.read_api(
            s, code=code, start_date=start_date, end_date=end_date, limit=0)
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
        if len(diff) < 3:
            continue

        if macd[-1] > 0 and signal[-1] > 0:
            continue

        data = diff[-3:]
        if data[-3] < data[-2] < 0 < data[-1]:
            result.append(tmp[key])

    # 顯示輸出
    show_result(result)


@get_group.command('kdj')
@click.option('--scalar', '-s', default=4, type=click.IntRange(1, 8), help="9天資料的粒度")
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--use_weekly', is_flag=True, help='使用每週歷史資料表來計算')
@click.option('--use_monthly', is_flag=True, help='使用每月歷史資料表來計算')
def get_kdj(type_s, use_weekly, use_monthly, scalar):
    date_data = get_stock_dates()
    end_date = get_latest_stock_date(date_data.get("market_holiday", []))
    season_date = get_latest_season_date(end_date)

    n_day = 9
    start_date = end_date - timedelta(days=scalar*n_day)

    s = get_session()
    stock_list = db_mgr.stock.readall_api(s, type_s=type_s, is_alive=True)
    if len(stock_list) == 0:
        return

    if use_weekly and use_monthly:
        click.echo("無法同時指定兩個歷史資料表")
        return

    if use_weekly:
        table = db_mgr.stock_weekly_history
        start_date = start_date - timedelta(days=(scalar*n_day*7)-(scalar*n_day))
    elif use_monthly:
        table = db_mgr.stock_monthly_history
        start_date = start_date - timedelta(days=(scalar*n_day*30)-(scalar*n_day))
    else:
        table = db_mgr.stock_daily_history

    result = []
    for stock in stock_list:
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

    # 顯示輸出
    show_result(result)
