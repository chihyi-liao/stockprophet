import click

from stockprophet.cli.ba.core.pbr import do_get_pbr
from stockprophet.cli.ba.core.eps import do_get_eps
from stockprophet.cli.ba.core.opm import do_get_opm
from stockprophet.cli.ba.core.balance import do_get_balance
from stockprophet.cli.common import show_result


@click.group()
def ba_group():
    pass


# --- get --- #
@ba_group.group('get')
def get_group():
    pass


@get_group.command('pbr')
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--rate_more', '-m', default=0.5, type=float, help="股價淨值比大於設定值")
@click.option('--rate_less', '-l', default=1.0, type=float, help="股價淨值比小於設定值")
def get_pbr(type_s: str, rate_more: float, rate_less: float):
    data = do_get_pbr(type_s=type_s, rate_more=rate_more, rate_less=rate_less, progress=True)

    # 輸出顯示
    if len(data) > 0:
        show_result(result=data)


@get_group.command('eps')
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--rate_more', '-m', default=3.0, type=float, help="eps大於設定值")
@click.option('--rate_less', '-l', default=10.0, type=float, help="eps小於設定值")
def get_eps(type_s: str, rate_more: float, rate_less: float):
    data = do_get_eps(type_s=type_s, rate_more=rate_more, rate_less=rate_less, progress=True)

    # 輸出顯示
    if len(data) > 0:
        show_result(result=data)


@get_group.command('balance')
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--liabs_count', '-l', default=3, type=int, help="連續 n 季負債減少")
@click.option('--asserts_count', '-a', default=3, type=int, help="連續 n 季資產增加")
def get_balance(type_s: str, liabs_count: int, asserts_count: int):
    data = do_get_balance(type_s=type_s, liabs_count=liabs_count, asserts_count=asserts_count, progress=True)

    # 輸出顯示
    if len(data) > 0:
        show_result(result=data)


@get_group.command('opm')
@click.option('--type_s', '-t', help="指定股市為上市或上櫃", type=click.Choice(['tse', 'otc']))
@click.option('--rate_more', '-m', default=10.0, type=float, help="營益率大於設定值")
@click.option('--rate_less', '-l', default=100.0, type=float, help="營益率小於設定值")
def get_opm(type_s: str, rate_more: float, rate_less: float):
    data = do_get_opm(type_s=type_s, rate_more=rate_more, rate_less=rate_less, progress=True)

    # 輸出顯示
    if len(data) > 0:
        show_result(result=data)
