from datetime import timedelta, date
from typing import Tuple
import click

from stockprophet.cli.ta import compute
from stockprophet.crawler.utils.date import get_latest_stock_date, get_latest_season_date
from stockprophet.crawler.utils.common import get_stock_dates
from stockprophet.db import get_session
from stockprophet.db import manager as db_mgr


@click.group()
def ta_group():
    pass


# --- get --- #
@ta_group.group('get')
def get_group():
    pass


def get_pbr(current_date, code) -> str:
    special_common_stocks = {'4157': 0.001 * 30, '6548': 1.0, '8070': 1.0}
    par_value = special_common_stocks.get(code, 10.0)
    s = get_session()
    history_list = db_mgr.stock_daily_history.read_api(
        s, code, start_date=current_date, end_date=current_date, limit=1)
    if len(history_list) != 1:
        return ''

    history = history_list[0]
    if not history['co']:
        return ''

    season_date = get_latest_season_date(current_date)
    balance_list = db_mgr.stock_balance_sheet.read_api(
            s, code, start_date=season_date, end_date=season_date, limit=1)
    if len(balance_list) == 1:
        balance = balance_list[0]
        shareholders_net_income = balance.get('shareholders_net_income')
        common_stocks = balance.get('common_stocks')
        if shareholders_net_income and common_stocks:
            p = shareholders_net_income / (common_stocks/par_value)  # 每股淨值
            pbr = round(history['co']/p, 2)  # 股價淨值比
            return str(pbr)
        total_assets = balance.get('total_assets')
        total_liabs = balance.get('total_liabs')
        if total_assets and total_liabs and common_stocks:
            p = (total_assets - total_liabs) / (common_stocks/par_value)  # 每股淨值
            pbr = round(history['co']/p, 2)  # 股價淨值比
            return str(pbr)
    return ''


@get_group.command('macd')
@click.option('--start_date', '-s', help="設定開始日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--end_date', '-e', help="設定結束日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--n_day', '-n', default=60, type=click.IntRange(1, 90), help="資料表計算天數")
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--use_weekly', is_flag=True, help='使用每週歷史資料表來計算')
@click.option('--use_monthly', is_flag=True, help='使用每月歷史資料表來計算')
def get_macd(start_date, end_date, n_day, type_s, use_weekly, use_monthly):
    # 設定初始日期
    if not end_date:
        date_data = get_stock_dates()
        end_date = get_latest_stock_date(date_data.get("market_holiday", []))

    if not start_date:
        start_date = end_date - timedelta(days=n_day)

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

    result = dict()
    for stock in stock_list:
        code = stock['code']
        name = stock['name']
        key = "%s(%s)" % (name, code)
        stock_data = table.read_api(
            s, code=code, start_date=start_date, end_date=end_date, limit=90)
        values = []
        tmp = dict()
        for i, v in enumerate(stock_data):
            if not v['co']:
                continue

            if i == len(stock_data) - 1:  # 取得最後的值
                pbr = get_pbr(end_date, code)
                tmp[key] = [v['op'], v['hi'], v['lo'], v['co'], v['ch'], int(v['vol']/1000), pbr]
            values.append(v['co'])

        if not tmp.get(key):
            continue

        macd, signal, diff = compute.macd(values)
        if len(diff) < 3:
            continue

        if macd[-1] > 0 and signal[-1] > 0:
            continue

        data = diff[-3:]
        if data[-3] < data[-2] < 0 < data[-1]:
            result[key] = tmp[key]

    # 顯示輸出
    header = "{:<16}\t{:>4}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}".format(
        "name(code)", chr(12288), "open", "high", "low", "close", "diff(%)", "volume", "pbr")
    line = "{:<16}\t{:>4}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}".format(
        "="*16, chr(12288), "="*8, "="*8, "="*8, "="*8, "="*8, "="*8, "="*8)
    click.echo(line)
    click.echo(header)
    click.echo(line)
    for k, v in result.items():
        msg = "{:<16}\t{:>4}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}".format(
            k, chr(12288), v[0], v[1], v[2], v[3], str(v[4])+'%', v[5], v[6])
        click.echo(msg)
