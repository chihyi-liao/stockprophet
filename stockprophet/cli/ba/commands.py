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

            income_list = db_mgr.stock_income_statement.read_api(
                s, code, start_date=season_date, end_date=season_date, limit=1)

            if len(income_list) == 0:
                continue

            income = income_list[0]
            eps = income.get('eps')

            if pbr is not None and rate_more < pbr < rate_less:
                result.append([
                    code, name, val['op'], val['hi'], val['lo'], val['co'],
                    val['ch'], int(val['vol']/1000), pbr, eps])

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
        collect = False
        if liabs_count:
            balance_list = db_mgr.stock_balance_sheet.read_api(
                s, code=code, end_date=season_date, order_desc=True, limit=liabs_count)
            if len(balance_list) != liabs_count:
                continue

            num = 0
            # 找尋連 n 季 負債減少的股票
            for balance in balance_list:
                liabs = balance['total_liabs']
                if not liabs:
                    continue

                if num == 0:
                    num = liabs
                else:
                    if liabs < num:
                        collect = False
                        break
                    else:
                        num = liabs
                        collect = True

        if not collect:
            continue

        collect = False
        if asserts_count:
            balance_list = db_mgr.stock_balance_sheet.read_api(
                s, code=code, end_date=season_date, order_desc=True, limit=asserts_count)
            if len(balance_list) != asserts_count:
                continue

            num = 0
            # 找尋連 n 季 資產增加的股票
            for balance in balance_list:
                asserts = balance['total_assets']
                if not asserts:
                    continue

                if num == 0:
                    num = asserts
                else:
                    if asserts > num:
                        collect = False
                        break
                    else:
                        num = asserts
                        collect = True

        if collect:
            history_list = db_mgr.stock_daily_history.read_api(
                s, code=code, start_date=latest_date, end_date=latest_date, limit=1)

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

                result.append([
                    code, name, val['op'], val['hi'], val['lo'], val['co'],
                    val['ch'], int(val['vol']/1000), pbr, eps
                ])

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
        net_sales = income['net_sales']
        op_income = income['operating_income']
        eps = income.get('eps')
        if net_sales and op_income:
            op_margin = round((op_income / net_sales) * 100.0, 2)
            if rate_more <= op_margin <= rate_less:
                result.append([
                    code, name, val['op'], val['hi'], val['lo'], val['co'],
                    val['ch'], int(val['vol'] / 1000), pbr, eps, op_margin])

    # 輸出顯示
    header = "{:<16}\t{:>4}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}".format(
        "name(code)", chr(12288), "open", "high", "low", "close", "diff(%)", "volume", "pbr", "eps", "op_margin" )
    line = "{:<16}\t{:>4}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}".format(
        "="*16, chr(12288), "="*8, "="*8, "="*8, "="*8, "="*8, "="*8, "="*8, "="*8, "="*8)
    click.echo(line)
    click.echo(header)
    click.echo(line)
    for v in result:
        k = "%s(%s)" % (v[1], v[0])
        msg = "{:<16}\t{:>4}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}\t{:>8}".format(
            k, chr(12288), v[2], v[3], v[4], v[5], str(v[6])+'%', v[7], v[8], v[9], v[10])
        click.echo(msg)
