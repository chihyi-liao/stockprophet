import click

from .stock import commands as stock
from .ta import commands as ta
from .ba import commands as ba
from .db import commands as db
from .sim import commands as sim
from .recommender import commands as recommender


@click.group()
def entry_point():
    pass


entry_point.add_command(stock.stock_group, 'stock')
entry_point.add_command(ta.ta_group, 'ta')
entry_point.add_command(ba.ba_group, 'ba')
entry_point.add_command(db.db_group, 'db')
entry_point.add_command(sim.sim_group, 'sim')
entry_point.add_command(recommender.recommender_group, 'recommender')
