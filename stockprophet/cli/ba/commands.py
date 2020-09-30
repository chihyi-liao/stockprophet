import click

from stockprophet.crawler.utils.date import get_latest_stock_date, get_latest_season_date
from stockprophet.crawler.utils.common import get_stock_dates
from stockprophet.db import get_session
from stockprophet.db import manager as db_mgr


@click.group()
def ba_group():
    pass


# --- get --- #
@ba_group.group('get')
def get_group():
    pass


@get_group.command('pbr')
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--rate_more', '-m', default=0.0, type=float, help="股價淨值比大於設定值")
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

        if len(balance_list) == 0:
            continue

        balance = balance_list[0]
        shareholders_net_income = balance.get('shareholders_net_income')
        common_stocks = balance.get('common_stocks')
        total_assets = balance.get('total_assets')
        total_liabs = balance.get('total_liabs')
        special_common_stocks = {'4157': 0.001 * 30, '6548': 1.0, '8070': 1.0}
        par_value = special_common_stocks.get(code, 10.0)
        if common_stocks:
            if shareholders_net_income:
                p = shareholders_net_income / (common_stocks / par_value)  # 每股淨值
                pbr = round(val['co'] / p, 2)  # 股價淨值比
            elif total_assets and total_liabs:
                p = (total_assets - total_liabs) / (common_stocks / par_value)  # 每股淨值
                pbr = round(val['co'] / p, 2)  # 股價淨值比
            else:
                pbr = None

            if pbr is not None and rate_more < pbr < rate_less:
                result.append([
                    code, name, val['op'], val['hi'], val['lo'], val['co'],
                    val['ch'], int(val['vol']/1000), pbr])

    # 輸出顯示
    header = "{:<16}\t{:>4}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}".format(
        "name(code)", chr(12288), "open", "high", "low", "close", "diff(%)", "volume", "pbr")
    line = "{:<16}\t{:>4}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}".format(
        "="*16, chr(12288), "="*8, "="*8, "="*8, "="*8, "="*8, "="*8, "="*8)
    click.echo(line)
    click.echo(header)
    click.echo(line)
    for v in result:
        k = "%s(%s)" % (v[1], v[0])
        msg = "{:<16}\t{:>4}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}".format(
            k, chr(12288), v[2], v[3], v[4], v[5], str(v[6])+'%', v[7], v[8])
        click.echo(msg)


@get_group.command('eps')
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--rate_more', '-m', default=0.0, type=float, help="eps大於設定值")
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

        balance_list = db_mgr.stock_balance_sheet.read_api(
            s, code, start_date=season_date, end_date=season_date, limit=1)

        if len(balance_list) == 0:
            continue

        balance = balance_list[0]
        shareholders_net_income = balance.get('shareholders_net_income')
        common_stocks = balance.get('common_stocks')
        total_assets = balance.get('total_assets')
        total_liabs = balance.get('total_liabs')
        special_common_stocks = {'4157': 0.001 * 30, '6548': 1.0, '8070': 1.0}
        par_value = special_common_stocks.get(code, 10.0)
        if common_stocks:
            if shareholders_net_income:
                p = shareholders_net_income / (common_stocks / par_value)  # 每股淨值
                pbr = round(val['co'] / p, 2)  # 股價淨值比
            elif total_assets and total_liabs:
                p = (total_assets - total_liabs) / (common_stocks / par_value)  # 每股淨值
                pbr = round(val['co'] / p, 2)  # 股價淨值比
            else:
                pbr = None

        income_list = db_mgr.stock_income_statement.read_api(
            s, code, start_date=season_date, end_date=season_date, limit=1)

        if len(income_list) == 0:
            continue

        income = income_list[0]
        eps = income.get('eps')
        if eps and rate_more <= eps <= rate_less:
            result.append([
                code, name, val['op'], val['hi'], val['lo'], val['co'],
                val['ch'], int(val['vol'] / 1000), pbr, eps])

    # 輸出顯示
    header = "{:<16}\t{:>4}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}".format(
        "name(code)", chr(12288), "open", "high", "low", "close", "diff(%)", "volume", "pbr", "eps")
    line = "{:<16}\t{:>4}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}".format(
        "="*16, chr(12288), "="*8, "="*8, "="*8, "="*8, "="*8, "="*8, "="*8, "="*8)
    click.echo(line)
    click.echo(header)
    click.echo(line)
    for v in result:
        k = "%s(%s)" % (v[1], v[0])
        msg = "{:<16}\t{:>4}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}".format(
            k, chr(12288), v[2], v[3], v[4], v[5], str(v[6])+'%', v[7], v[8], v[9])
        click.echo(msg)
