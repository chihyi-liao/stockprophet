from datetime import datetime, timedelta
import click

from stockprophet.cli.ta import compute
from stockprophet.cli.common import show_simulate_result
from stockprophet.crawler.utils.date import date_range, is_weekday
from stockprophet.crawler.utils.common import get_stock_dates
from stockprophet.db import get_session
from stockprophet.db import manager as db_mgr


Kilo = 1000
Fee = 1.425 / Kilo
Tax = 3.0 / Kilo


def check_macd_buy_point(macd: list, signal: list, diff: list) -> bool:
    result = False
    if len(diff) < 3:
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


@click.group()
def sim_group():
    pass


@sim_group.command('macd')
@click.option('--code', '-c', help="指定股票代號", type=str, required=True)
@click.option('--principal', '-p', help="模擬本金", default=50*10000, type=int, required=True)
@click.option('--init_vol', '-v', help="初始買量", default=2, type=int, required=True)
@click.option('--start_date', '-sd', help="設定模擬開始日期", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option('--end_date', '-ed', help="設定模擬結束日期", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option('--n_day', '-n', default=90, show_default=True, type=click.IntRange(1, 120), help="資料表計算天數")
@click.option('--use_weekly', is_flag=True, help='使用每週歷史資料表來計算')
@click.option('--use_monthly', is_flag=True, help='使用每月歷史資料表來計算')
@click.option('--fast', '-f', default=12, show_default=True, type=int, help='macd 快線參數')
@click.option('--slow', '-s', default=26, show_default=True, type=int, help='macd 慢線參數')
@click.option('--dif', '-d', default=9, show_default=True, type=int, help='macd 差離值參數')
def get_macd(code, principal, init_vol, start_date, end_date, n_day, use_weekly, use_monthly, fast, slow, dif):
    date_data = get_stock_dates()
    # 取得所有節慶的休市日期(包含颱風假)
    holiday_list = []
    for str_date in date_data.get("market_holiday", []):
        holiday_list.append(datetime.strptime(str_date, "%Y-%m-%d").date())

    # 取得補上班日的股市日期
    trading_list = []
    for str_date in date_data.get("additional_trading_day", []):
        trading_list.append(datetime.strptime(str_date, "%Y-%m-%d").date())

    if start_date >= end_date:
        click.echo("錯誤的開始與結束日期")
        return

    if use_weekly and use_monthly:
        click.echo("無法同時指定兩個歷史資料表")
        return

    if use_weekly:
        table = db_mgr.stock_weekly_history
    elif use_monthly:
        table = db_mgr.stock_monthly_history
    else:
        table = db_mgr.stock_daily_history

    # 初始變數
    origin_principal = principal
    volume = 0
    avg_price = 0  # 均價
    result = []
    s = get_session()
    for current_date in date_range(start_date=start_date.date(), end_date=end_date.date()):
        if current_date in holiday_list:
            continue

        # 當抓取日期是週末而且不是補上班日期則跳過
        if is_weekday(current_date) and current_date not in trading_list:
            continue

        if use_weekly:
            stock_start_date = current_date - timedelta(days=n_day*7-n_day)
        elif use_monthly:
            stock_start_date = current_date - timedelta(days=n_day*30-n_day)
        else:
            stock_start_date = current_date - timedelta(days=n_day)

        stock_data = table.read_api(
            s, code=code, start_date=stock_start_date, end_date=current_date, limit=0)

        values = []  # 收集每日收盤價
        for i, val in enumerate(stock_data):
            if not val['co']:
                continue
            values.append(val['co'])

        if len(values) == 0:
            continue

        price = values[-1]  # 當日股價
        date_str = current_date.strftime("%Y-%m-%d")
        macd, signal, diff = compute.macd(values, fast=fast, slow=slow, n=dif)

        # 買點判斷
        if check_macd_buy_point(macd=macd, signal=signal, diff=diff) is True:
            # 計算本金夠買的量
            buy_vol = int(principal / (price * Kilo))
            if not volume:
                # 若沒有持股, 一開始以基本量購買
                if buy_vol > init_vol:
                    buy_vol = init_vol
            else:
                # 若持續下跌, 則攤平策略, 倍數乘買
                if init_vol * 2 < buy_vol:
                    init_vol *= 2
                    buy_vol = init_vol

            # 計算花費成本(包含交易稅)
            stock_cost = price * buy_vol * Kilo
            total_cost = stock_cost + int(stock_cost * Fee)
            if principal - total_cost > 0:
                volume += buy_vol
            else:
                buy_vol -= 1  # 加上交易稅之後不足買一張, 因此減少買量
                volume += buy_vol
                stock_cost = price * buy_vol * Kilo
                total_cost = stock_cost + int(stock_cost * Fee)
            principal -= total_cost  # 本金扣除股票市值

            # 計算平均價
            if avg_price == 0:
                avg_price = price
            else:
                # ex:    no. price   vol.    avg.
                #         1     20     4      20
                #         2     10     2    16.6 => ((6 - 2) * 20 + (2 * 10)) / 6
                before_cost = (volume - buy_vol) * avg_price
                after_cost = buy_vol * price
                avg_price = round((before_cost + after_cost) / volume, 2)

            # 若有買量則加入資料集作為輸出
            if buy_vol > 0:
                stock_assets = volume * avg_price * Kilo  # 計算股票市值
                total_assets = principal + stock_assets   # 計算總資產
                roi = round((total_assets - origin_principal) / origin_principal * 100, 2)  # 計算投資報酬率
                data = [
                    date_str, price, buy_vol, '-', '-', avg_price,
                    volume, int(stock_assets), int(total_assets), roi]
                result.append(data)

        # 賣點判斷
        if check_macd_sell_point(macd=macd, signal=signal, diff=diff) is True:
            # 若賣點低於平均值, 則認賠一半否則全部賣出
            if price < avg_price:
                sell_vol = int(volume / 2)
            else:
                sell_vol = volume

            # 若有賣量則加入資料集作為輸出
            if sell_vol > 0:
                principal += (sell_vol * price * Kilo) - int((price * sell_vol * Kilo) * Tax)  # 更新本金
                volume -= sell_vol  # 總量減少
                # 初始化設定值
                if volume == 0:
                    avg_price = 0
                    init_vol = 2

                stock_assets = volume * avg_price * Kilo  # 計算股票市值
                total_assets = principal + stock_assets   # 計算總資產
                roi = round((total_assets - origin_principal) / origin_principal * 100, 2)
                data = [
                    date_str, '-', '-', price, sell_vol, avg_price,
                    volume, int(stock_assets), int(total_assets), roi]
                result.append(data)
    # 輸出結果
    show_simulate_result(result=result)
