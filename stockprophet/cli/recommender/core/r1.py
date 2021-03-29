from datetime import datetime, timedelta

from stockprophet.crawler.utils.date import get_latest_stock_date, get_latest_season_date, date_range
from stockprophet.crawler.utils.common import get_stock_dates
from stockprophet.db import get_session
from stockprophet.db.manager import sync_api as db_mgr
from stockprophet.cli.common import calc_pbr, calc_op_margin, calc_eps
from stockprophet.cli.ta.core import compute
from stockprophet.utils import get_logger


logger = get_logger(__name__)


def check_basic(s, code, latest_date, season_date, pbr, opm, eps):
    result = False
    history_list = db_mgr.stock_daily_history.read_api(
        s, code=code, start_date=latest_date, end_date=latest_date, limit=1)

    if len(history_list) == 0:
        return result

    val = history_list[0]
    if not val['co']:
        return result

    balance_list = db_mgr.stock_balance_sheet.read_api(
        s, code, start_date=season_date, end_date=season_date, limit=1)

    pbr_val = calc_pbr(code, val['co'], balance_list=balance_list)
    if not pbr_val or pbr_val > pbr:
        return result

    income_list = db_mgr.stock_income_statement.read_api(
        s, code, start_date=season_date, end_date=season_date, limit=1)

    op_margin = calc_op_margin(income_list=income_list)
    if not op_margin or op_margin < opm:
        return result

    eps_val = calc_eps(income_list=income_list)
    if not eps_val or eps_val < eps:
        return result

    prev_month_date = latest_date.replace(day=1) - timedelta(days=1)
    revenues = db_mgr.stock_monthly_revenue.read_api(s, code, end_date=prev_month_date, order_desc=True, limit=2)
    if len(revenues) != 2:
        return result

    last_year_date = datetime(year=prev_month_date.year-1, month=prev_month_date.month, day=1)
    last_year_revenues = db_mgr.stock_monthly_revenue.read_api(s, code, end_date=last_year_date,
                                                               order_desc=True, limit=1)
    if len(last_year_revenues) != 1:
        return result

    last_revenue = revenues[0]['revenue']
    prev_revenue = revenues[1]['revenue']
    last_year_revenue = last_year_revenues[0]['revenue']
    if last_revenue + int(last_revenue * 1 / 100) < prev_revenue and last_revenue < last_year_revenue:
        result = False
    else:
        result = True
    return result


def check_tech(s, code, latest_date, avg_vol=300):
    result = False
    start_date = latest_date - timedelta(days=90)

    history_list = db_mgr.stock_daily_history.read_api(
        s, code=code, start_date=start_date, end_date=latest_date, limit=0)
    if len(history_list) == 0:
        return result, ''

    close_values = []
    high_values = []
    low_values = []
    volumes = []
    for i, val in enumerate(history_list):
        if not val['co']:
            continue

        high_values.append(val['hi'])
        low_values.append(val['lo'])
        close_values.append(val['co'])
        volumes.append(int(val['vol'] / 1000))

    # 檢查MA交易量是否到判斷條件
    ma5_volumes = compute.sma(volumes, 5)
    if len(ma5_volumes) > 0 and ma5_volumes[-1] < avg_vol:
        return result, ''

    # 檢查KD線是否到判斷條件
    k_data, d_data, j_data = compute.kdj(high_values, low_values, close_values, 9)
    if len(k_data) > 2 and k_data[-1] <= 40:
        if k_data[-2] - d_data[-2] <= 0 <= k_data[-1] - d_data[-1]:
            result = True
            return result, 'KD'

    # 檢查MACD線是否到判斷條件
    macd, signal, diff = compute.macd(close_values, fast=12, slow=26, n=9)
    if len(diff) >= 2 and macd[-1] < 0:
        diff_data = diff[-2:]
        if diff_data[-2] <= diff_data[-1] <= 0 < diff_data[-1] + 0.01:
            result = True
            return result, 'MACD'

    # 檢查MA線是否到判斷條件
    ma5_prices = compute.sma(close_values, 5)
    ma10_prices = compute.sma(close_values, 10)
    if len(ma5_prices) > 1 and len(ma10_prices) > 1:
        if ma5_prices[-1] > ma10_prices[-1] and ma5_prices[-2] < ma10_prices[-2]:
            result = True
            return result, 'MA'

    return result, ''


def do_recommendation1(type_s: str, set_date: datetime, pbr: float, opm: float, eps: float) -> list:
    result = []

    if set_date:
        latest_date = set_date
    else:
        # 取得最新交易日
        date_data = get_stock_dates()
        latest_date = get_latest_stock_date(date_data.get("market_holiday", []))
    season_date = get_latest_season_date(latest_date)

    s = get_session()
    stock_list = db_mgr.stock.readall_api(s, type_s=type_s, is_alive=True)
    if len(stock_list) == 0:
        return result

    for cur, stock in enumerate(stock_list):
        # 檢查基本面
        if not check_basic(s, stock['code'], latest_date, season_date, pbr, opm, eps):
            continue

        # 檢查技術面
        tech_result, reason = check_tech(s, stock['code'], latest_date)
        if not tech_result:
            continue

        history_list = db_mgr.stock_daily_history.read_api(
            s, code=stock['code'], start_date=latest_date, end_date=latest_date, limit=1)
        if len(history_list) == 0:
            continue
        if history_list[0]['co']:
            result.append(
                tuple([
                    stock['id'], stock['code'], stock['name'], reason,
                    history_list[0]['co'], latest_date.strftime("%Y-%m-%d")
                ]))
    return result


def do_recommendation1_table(type_s: str, pbr: float, opm: float, eps: float,
                             start_date: datetime, end_date: datetime):
    s = get_session()
    for current_date in date_range(start_date=start_date, end_date=end_date):
        logger.info("找尋 %s 建議股市" % (current_date.strftime("%Y-%m-%d"),))
        data = do_recommendation1(type_s=type_s, set_date=current_date, pbr=pbr, opm=opm, eps=eps)
        if data:
            insert_data = []
            stock_date_list = db_mgr.stock_daily_date.read_api(s, current_date)
            if len(stock_date_list) == 0:
                continue

            stock_date_id = stock_date_list[0]['id']
            for stock_id, code, _, reason, price, _ in data:
                recommends = db_mgr.stock_recommendation.read_api(
                    s, code, start_date=current_date, end_date=current_date, limit=1)
                if len(recommends) == 0:
                    insert_data.append(
                        {'price': price, 'note': reason, 'stock_id': stock_id, 'stock_date_id': stock_date_id})

            if len(insert_data) > 0:
                logger.info("建立 %s 建議股市資料: %s" % (current_date.strftime("%Y-%m-%d"), insert_data))
                db_mgr.stock_recommendation.create_api(s, insert_data)
