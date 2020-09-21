import click

from stockprophet.crawler import tse, otc, mops


@click.group()
def stock_group():
    pass


# --- build --- #
@stock_group.group('build')
def build_group():
    pass


@build_group.command()
@click.option('--tse_start_date', '-ts', help="設定開始日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--tse_end_date', '-te', help="設定結束日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--otc_start_date', '-os', help="設定開始日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--otc_end_date', '-oe', help="設定結束日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
def daily_history_table(tse_start_date, tse_end_date, otc_start_date, otc_end_date, type_s):
    click.echo("執行上市上櫃爬蟲")
    if type_s == 'tse':
        tse_task = tse.CrawlerTask(start_date=tse_start_date, end_date=tse_end_date)
        tse_task.start()
        tse_task.join()
    elif type_s == 'otc':
        otc_task = otc.CrawlerTask(start_date=otc_start_date, end_date=otc_end_date)
        otc_task.start()
        otc_task.join()
    else:
        tse_task = tse.CrawlerTask(start_date=tse_start_date, end_date=tse_end_date)
        otc_task = otc.CrawlerTask(start_date=otc_start_date, end_date=otc_end_date)
        tse_task.start()
        otc_task.start()
        tse_task.join()
        otc_task.join()


@build_group.command()
@click.option('--tse_start_date', '-ts', help="設定開始日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--tse_end_date', '-te', help="設定結束日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--otc_start_date', '-os', help="設定開始日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--otc_end_date', '-oe', help="設定結束日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
def period_history_table(tse_start_date, tse_end_date, otc_start_date, otc_end_date, type_s):
    click.echo("建立上市上櫃週期報表")
    if type_s == 'tse':
        tse_task = tse.CrawlerTask(start_date=tse_start_date, end_date=tse_end_date, build_period_table=True)
        tse_task.start()
        tse_task.join()
    elif type_s == 'otc':
        otc_task = otc.CrawlerTask(start_date=otc_start_date, end_date=otc_end_date, build_period_table=True)
        otc_task.start()
        otc_task.join()
    else:
        tse_task = tse.CrawlerTask(start_date=tse_start_date, end_date=tse_end_date, build_period_table=True)
        otc_task = otc.CrawlerTask(start_date=otc_start_date, end_date=otc_end_date, build_period_table=True)
        tse_task.start()
        otc_task.start()
        tse_task.join()
        otc_task.join()


@build_group.command()
@click.option('--start_date', '-s', help="設定開始日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--end_date', '-e', help="設定結束日期", type=click.DateTime(formats=["%Y-%m-%d"]))
def mops_income_statement_table(start_date, end_date):
    click.echo("建立上市上櫃綜合損益表")
    task = mops.CrawlerTask(start_date=start_date, end_date=end_date, build_income_table=True)
    task.start()
    task.join()


@build_group.command()
@click.option('--start_date', '-s', help="設定開始日期", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option('--end_date', '-e', help="設定結束日期", type=click.DateTime(formats=["%Y-%m-%d"]))
def mops_balance_sheet_table(start_date, end_date):
    click.echo("建立上市上櫃資產負債表")
    task = mops.CrawlerTask(start_date=start_date, end_date=end_date, build_balance_table=True)
    task.start()
    task.join()
