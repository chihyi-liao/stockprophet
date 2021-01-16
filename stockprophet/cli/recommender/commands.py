from datetime import datetime

import click

from stockprophet.crawler.utils.date import get_latest_stock_date
from stockprophet.crawler.utils.common import get_stock_dates
from .core.r1 import do_recommendation1, do_recommendation1_table

latest_date = get_latest_stock_date(get_stock_dates().get("market_holiday", []))


@click.group()
def recommender_group():
    pass


@recommender_group.command('r1')
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--date', '-d', help="設定日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--pbr', '-p', default=1.5, type=float, show_default=True, help="股價淨值比小於設定值")
@click.option('--opm', '-o', default=0.0, type=float, show_default=True, help="營益率大於設定值")
@click.option('--eps', '-e', default=0.0, type=float, show_default=True, help="EPS大於設定值")
def recommendation_1(type_s: str, date: datetime, pbr: float, opm: float, eps: float):
    data = do_recommendation1(type_s=type_s, set_date=date, pbr=pbr, opm=opm, eps=eps)
    click.echo(data)


@recommender_group.command('r1_table')
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--pbr', '-p', default=1.5, type=float, show_default=True, help="股價淨值比小於設定值")
@click.option('--opm', '-o', default=0.0, type=float, show_default=True, help="營益率大於設定值")
@click.option('--eps', '-e', default=0.0, type=float, show_default=True, help="EPS大於設定值")
@click.option('--start_date', '-sd', help="設定模擬開始日期", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option('--end_date', '-ed', help="設定模擬結束日期", default=latest_date.strftime("%Y-%m-%d"), show_default=True,
              type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
def recommendation_1_table(type_s: str, pbr: float, opm: float, eps: float,
                           start_date: datetime, end_date: datetime):
    do_recommendation1_table(type_s=type_s, pbr=pbr, opm=opm, eps=eps, start_date=start_date, end_date=end_date)
