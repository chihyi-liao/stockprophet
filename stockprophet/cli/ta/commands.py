import click

from stockprophet.cli.common import show_result
from stockprophet.cli.ta.core.macd import do_get_macd
from stockprophet.cli.ta.core.kdj import do_get_kdj


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
    data = do_get_macd(
        n_day=n_day, type_s=type_s, use_weekly=use_weekly, use_monthly=use_monthly,
        fast=fast, slow=slow, dif=dif, progress=True)

    # 顯示輸出
    if len(data) > 0:
        show_result(result=data)


@get_group.command('kdj')
@click.option('--n_day', '-n', default=9, show_default=True, type=click.IntRange(3, 12), help="資料表計算天數")
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--use_weekly', is_flag=True, help='使用每週歷史資料表來計算')
@click.option('--use_monthly', is_flag=True, help='使用每月歷史資料表來計算')
@click.option('--scalar', '-s', default=5, type=click.IntRange(1, 8), help="n_day資料的純量")
def get_kdj(n_day: int, type_s: str, use_weekly: bool, use_monthly: bool, scalar: int):
    data = do_get_kdj(
        n_day=n_day, type_s=type_s, use_weekly=use_weekly, use_monthly=use_monthly,
        scalar=scalar, progress=True)

    # 顯示輸出
    if len(data) > 0:
        show_result(result=data)
