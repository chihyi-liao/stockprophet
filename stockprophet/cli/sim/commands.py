import click

from stockprophet.cli.common import show_simulate_result, show_simulate_top_result
from .core.macd import do_macd, do_all_macd


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
def get_macd(code, principal, init_vol, start_date, end_date, n_day,
             use_weekly, use_monthly, fast, slow, dif):
    data = do_macd(
        code=code, principal=principal, init_vol=init_vol, start_date=start_date.date(), end_date=end_date.date(),
        n_day=n_day, use_weekly=use_weekly, use_monthly=use_monthly, fast=fast, slow=slow, dif=dif, progress=True)

    # 顯示模擬結果
    if len(data) > 0:
        show_simulate_result(result=data)


@sim_group.command('all_macd')
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
@click.option('--limit_price', '-l', default=25, type=int, help='股價低於設定值')
@click.option('--top_size', '-t', default=20, type=int, help='設定top大小')
def get_all_macd(principal, init_vol, start_date, end_date, n_day,
                 use_weekly, use_monthly, fast, slow, dif, top_size, limit_price):
    data = do_all_macd(
        principal=principal, init_vol=init_vol, start_date=start_date.date(), end_date=end_date.date(),
        n_day=n_day, use_weekly=use_weekly, use_monthly=use_monthly, fast=fast, slow=slow, dif=dif,
        top_size=top_size, limit_price=limit_price, progress=True)

    # 顯示模擬結果
    if len(data) > 0:
        show_simulate_top_result(result=data)
