from datetime import datetime, timedelta
from typing import List, Optional

import click

from stockprophet.cli.ta import compute, account
from stockprophet.cli.common import show_simulate_result
from stockprophet.crawler.utils.date import date_range, is_weekday
from stockprophet.crawler.utils.common import get_stock_dates
from stockprophet.db import get_session
from stockprophet.db import manager as db_mgr


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


def get_roi(price: float, avg_price: float) -> float:
    return round((price - avg_price) / avg_price * 100, 2)


@click.group()
def sim_group():
    pass


@sim_group.command('macd')
@click.option('--code', '-c', help="指定股票代號", type=str, required=True)
@click.option('--principal', '-p', help="模擬本金", default=50*10000, type=int, required=True)
@click.option('--init_vol', '-v', help="初始買量", default=2, type=int, required=True)
@click.option('--start_date', '-sd', help="設定模擬開始日期", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option('--end_date', '-ed', help="設定模擬結束日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--n_day', '-n', default=90, show_default=True, type=click.IntRange(1, 120), help="資料表計算天數")
@click.option('--use_weekly', is_flag=True, help='使用每週歷史資料表來計算')
@click.option('--use_monthly', is_flag=True, help='使用每月歷史資料表來計算')
@click.option('--fast', '-f', default=12, show_default=True, type=int, help='macd 快線參數')
@click.option('--slow', '-s', default=26, show_default=True, type=int, help='macd 慢線參數')
@click.option('--dif', '-d', default=9, show_default=True, type=int, help='macd 差離值參數')
@click.option('--show_result', default=True, is_flag=True, help='顯示交易紀錄')
def get_macd(code, principal, init_vol, start_date, end_date, n_day,
             use_weekly, use_monthly, fast, slow, dif, show_result) -> int:
    if not end_date:
        end_date = datetime.today()

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
        return 0

    if use_weekly and use_monthly:
        click.echo("無法同時指定兩個歷史資料表")
        return 0

    if use_weekly:
        table = db_mgr.stock_weekly_history
    elif use_monthly:
        table = db_mgr.stock_monthly_history
    else:
        table = db_mgr.stock_daily_history

    # 初始變數
    my_account = account.Account(principal=principal)
    last_price = 0.0
    last_date = None
    tmp_vol = init_vol

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
        last_price = price
        last_date = current_date
        macd, signal, diff = compute.macd(values, fast=fast, slow=slow, n=dif)

        # 買點判斷
        if check_macd_buy_point(macd=macd, signal=signal, diff=diff) is True:
            # 計算本金夠買的量
            max_vol = my_account.get_max_buy_volume(price=price)
            if my_account.total_volume == 0:
                vol = init_vol if max_vol >= init_vol else max_vol
                my_account.buy(current_date, price=price, volume=vol)
            else:
                vol = tmp_vol * 2
                if max_vol >= vol:
                    tmp_vol = vol
                    my_account.buy(current_date, price=price, volume=vol)

        # 賣點判斷
        if check_macd_sell_point(macd=macd, signal=signal, diff=diff) is True:
            # 若賣點低於平均值, 則認賠一半否則全部賣出
            total_volume = my_account.get_total_volume()
            if price < my_account.get_avg_price():
                sell_vol = int(total_volume / 2)
            else:
                tmp_vol = init_vol
                sell_vol = total_volume

            if sell_vol > 0:
                my_account.sell(current_date, price=price, volume=sell_vol)

    # 最後一天賣出總結
    total_volume = my_account.get_total_volume()
    if total_volume > 0 and last_date:
        my_account.sell(last_date, last_price, total_volume)

    # 輸出結果
    if show_result:
        show_simulate_result(result=my_account.get_records())
    return my_account.total_assets


@sim_group.command('all_macd')
@click.option('--principal', '-p', help="模擬本金", default=50*10000, type=int, required=True)
@click.option('--init_vol', '-v', help="初始買量", default=2, type=int, required=True)
@click.option('--start_date', '-sd', help="設定模擬開始日期", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option('--end_date', '-ed', help="設定模擬結束日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--n_day', '-n', default=90, show_default=True, type=click.IntRange(1, 120), help="資料表計算天數")
@click.option('--use_weekly', is_flag=True, help='使用每週歷史資料表來計算')
@click.option('--use_monthly', is_flag=True, help='使用每月歷史資料表來計算')
@click.option('--fast', '-f', default=12, show_default=True, type=int, help='macd 快線參數')
@click.option('--slow', '-s', default=26, show_default=True, type=int, help='macd 慢線參數')
@click.option('--dif', '-d', default=9, show_default=True, type=int, help='macd 差離值參數')
@click.option('--show_result', default=False, is_flag=False, help='顯示交易紀錄')
@click.pass_context
def get_all_macd(ctx, principal, init_vol, start_date, end_date, n_day,
                 use_weekly, use_monthly, fast, slow, dif, show_result):
    ctx.forward(get_macd)
    top10: Optional[List[None, int]] = [None for _ in range(10)]
    code10: Optional[List[str]] = ['' for _ in range(10)]
    count = 0
    s = get_session()
    stock_list = db_mgr.stock.readall_api(s, is_alive=True)
    with click.progressbar(length=len(stock_list), label='TOP10 MACD') as bar:
        for progress, stock in enumerate(stock_list):
            bar.update(progress)
            code = stock['code']
            name = stock['name']
            macd_value = ctx.invoke(get_macd, code=code, principal=principal, init_vol=init_vol,
                                    start_date=start_date, end_date=end_date, n_day=n_day,
                                    use_weekly=use_weekly, use_monthly=use_monthly,
                                    fast=fast, slow=slow, dif=dif, show_result=show_result)
            if macd_value > principal:
                if count < 10:
                    for i in range(len(top10)):
                        if top10[i] is None:
                            top10[i] = macd_value
                            code10[i] = "%s(%s)" % (code, name)
                            count += 1
                            break
                else:
                    min_val = None
                    min_idx = None
                    for i in range(len(top10)):
                        if min_val is None:
                            min_val = top10[i]
                            min_idx = i
                        else:
                            if top10[i] < min_val:
                                min_val = top10[i]
                                min_idx = i

                    if macd_value > min_val:
                        top10[min_idx] = macd_value
                        code10[min_idx] = "%s(%s)" % (code, name)

    # 輸出結果
    for i in range(len(top10)):
        msg = "no%d: %s, 總資產: %s" % (i+1, code10[i], top10[i])
        click.echo(msg)
