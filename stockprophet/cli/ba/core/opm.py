from stockprophet.cli.common import progressbar
from stockprophet.crawler.utils.date import get_latest_stock_date, get_latest_season_date
from stockprophet.crawler.utils.common import get_stock_dates
from stockprophet.db import get_session
from stockprophet.db import manager as db_mgr
from stockprophet.cli.common import calc_pbr, calc_gross_margin, calc_op_margin, calc_eps


def do_get_opm(type_s: str, rate_more: float, rate_less: float, progress: bool = False) -> list:
    result = []
    if rate_more > rate_less:
        return result

    # 取得最新交易日
    date_data = get_stock_dates()
    latest_date = get_latest_stock_date(date_data.get("market_holiday", []))
    season_date = get_latest_season_date(latest_date)

    s = get_session()
    stock_list = db_mgr.stock.readall_api(s, type_s=type_s, is_alive=True)
    if len(stock_list) == 0:
        return result

    total = len(stock_list)
    for cur, stock in enumerate(stock_list):
        # 處理進度條
        if progress:
            progressbar(cur=cur+1, total=total)

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

    return result
