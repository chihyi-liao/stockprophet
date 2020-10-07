import click

from stockprophet.crawler.utils.date import get_latest_stock_date, get_latest_season_date
from stockprophet.crawler.utils.common import get_stock_dates
from stockprophet.db import get_session
from stockprophet.db import manager as db_mgr
from stockprophet.cli.common import (
    show_result, calc_pbr, calc_gross_margin, calc_op_margin, calc_eps,
    is_asserts_asc, is_liabs_desc
)


@click.group()
def ba_group():
    pass


# --- get --- #
@ba_group.group('get')
def get_group():
    pass


@get_group.command('pbr')
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--rate_more', '-m', default=0.5, type=float, help="股價淨值比大於設定值")
@click.option('--rate_less', '-l', default=1.0, type=float, help="股價淨值比小於設定值")
def get_pbr(type_s, rate_more, rate_less):
    if rate_more > rate_less:
        click.echo("rate參數錯誤: rate_more < pbr < rate_less")
        return

    # 取得最新交易日
    date_data = get_stock_dates()
    latest_date = get_latest_stock_date(date_data.get("market_holiday", []))
    season_date = get_latest_season_date(latest_date)

    s = get_session()
    stock_list = db_mgr.stock.readall_api(s, type_s=type_s, is_alive=True)
    if len(stock_list) == 0:
        return

    result = []
    for stock in stock_list:
        code = stock['code']
        name = stock['name']
        history_list = db_mgr.stock_daily_history.read_api(
            s, code, start_date=latest_date, end_date=latest_date, limit=1)
        if len(history_list) == 0:
            continue

        val = history_list[0]
        if not val['co']:
            continue

        balance_list = db_mgr.stock_balance_sheet.read_api(
            s, code, start_date=season_date, end_date=season_date, limit=1)

        pbr = calc_pbr(code, val['co'], balance_list=balance_list)
        if pbr:
            data = [code, name, val['co'], val['ch'], int(val['vol'] / 1000)]
            if pbr and rate_more < pbr < rate_less:
                data.append(pbr)
                income_list = db_mgr.stock_income_statement.read_api(
                    s, code, start_date=season_date, end_date=season_date, limit=1)

                eps = calc_eps(income_list=income_list)
                data.append(eps if eps else '')

                op_margin = calc_op_margin(income_list=income_list)
                data.append(op_margin if op_margin else '')

                gross_margin = calc_gross_margin(income_list=income_list)
                data.append(gross_margin if gross_margin else '')
                result.append(data)

    # 輸出顯示
    show_result(result)


@get_group.command('eps')
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--rate_more', '-m', default=3.0, type=float, help="eps大於設定值")
@click.option('--rate_less', '-l', default=10.0, type=float, help="eps小於設定值")
def get_eps(type_s, rate_more, rate_less):
    if rate_more > rate_less:
        click.echo("rate參數錯誤: rate_more < pbr < rate_less")
        return

    # 取得最新交易日
    date_data = get_stock_dates()
    latest_date = get_latest_stock_date(date_data.get("market_holiday", []))
    season_date = get_latest_season_date(latest_date)

    s = get_session()
    stock_list = db_mgr.stock.readall_api(s, type_s=type_s, is_alive=True)
    if len(stock_list) == 0:
        return

    result = []
    for stock in stock_list:
        code = stock['code']
        name = stock['name']
        history_list = db_mgr.stock_daily_history.read_api(
            s, code, start_date=latest_date, end_date=latest_date, limit=1)

        if len(history_list) == 0:
            continue

        val = history_list[0]
        if not val['co']:
            continue

        income_list = db_mgr.stock_income_statement.read_api(
            s, code, start_date=season_date, end_date=season_date, limit=1)

        eps = calc_eps(income_list=income_list)
        if eps and rate_more <= eps <= rate_less:
            data = [code, name, val['co'], val['ch'], int(val['vol'] / 1000)]
            balance_list = db_mgr.stock_balance_sheet.read_api(
                s, code, start_date=season_date, end_date=season_date, limit=1)

            pbr = calc_pbr(code, val['co'], balance_list=balance_list)
            data.append(pbr if pbr else '')
            data.append(eps if eps else '')

            op_margin = calc_op_margin(income_list=income_list)
            data.append(op_margin if op_margin else '')

            gross_margin = calc_gross_margin(income_list=income_list)
            data.append(gross_margin if gross_margin else '')
            result.append(data)

    # 輸出顯示
    show_result(result)


@get_group.command('balance')
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--liabs_count', '-l', default=3, type=int, help="連續 n 季負債減少")
@click.option('--asserts_count', '-a', default=3, type=int, help="連續 n 季資產增加")
def get_balance(type_s, liabs_count, asserts_count):
    # 取得最新交易日
    date_data = get_stock_dates()
    latest_date = get_latest_stock_date(date_data.get("market_holiday", []))
    season_date = get_latest_season_date(latest_date)

    s = get_session()
    stock_list = db_mgr.stock.readall_api(s, type_s=type_s, is_alive=True)
    if len(stock_list) == 0:
        return

    result = []
    for stock in stock_list:
        code = stock['code']
        name = stock['name']
        if liabs_count:
            balance_list = db_mgr.stock_balance_sheet.read_api(
                s, code=code, end_date=season_date, order_desc=True, limit=liabs_count)
            if not is_liabs_desc(balance_list=balance_list, n=liabs_count):
                continue

        if asserts_count:
            balance_list = db_mgr.stock_balance_sheet.read_api(
                s, code=code, end_date=season_date, order_desc=True, limit=asserts_count)
            if not is_asserts_asc(balance_list=balance_list, n=asserts_count):
                continue

        history_list = db_mgr.stock_daily_history.read_api(
            s, code=code, start_date=latest_date, end_date=latest_date, limit=1)

        if len(history_list) == 0:
            continue

        val = history_list[0]
        if not val['co']:
            continue

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
        result.append(data)
    # 輸出顯示
    show_result(result)


@get_group.command('op_margin')
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--rate_more', '-m', default=10.0, type=float, help="營益率大於設定值")
@click.option('--rate_less', '-l', default=100.0, type=float, help="營益率小於設定值")
def get_op_margin(type_s, rate_more, rate_less):
    if rate_more > rate_less:
        click.echo("rate參數錯誤: rate_more < op_margin < rate_less")
        return

    # 取得最新交易日
    date_data = get_stock_dates()
    latest_date = get_latest_stock_date(date_data.get("market_holiday", []))
    season_date = get_latest_season_date(latest_date)

    s = get_session()
    stock_list = db_mgr.stock.readall_api(s, type_s=type_s, is_alive=True)
    if len(stock_list) == 0:
        return

    result = []
    for stock in stock_list:
        code = stock['code']
        name = stock['name']
        history_list = db_mgr.stock_daily_history.read_api(
            s, code, start_date=latest_date, end_date=latest_date, limit=1)
        if len(history_list) == 0:
            continue

        val = history_list[0]
        if not val['co']:
            continue

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
        if op_margin and rate_more <= op_margin <= rate_less:
            data.append(op_margin)
            gross_margin = calc_gross_margin(income_list=income_list)
            data.append(gross_margin if gross_margin else '')
            result.append(data)

    # 輸出顯示
    show_result(result)
