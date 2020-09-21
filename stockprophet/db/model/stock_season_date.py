import datetime

from sqlalchemy import Table, Column
from sqlalchemy.orm import mapper
from sqlalchemy.types import Integer, Date

from stockprophet.db import metadata


class StockSeasonDate(object):
    __table__ = None

    def __init__(self, date: datetime.date):
        self.date = date

    def __repr__(self):
        return "<StockSeasonDate('%s')>" % (self.date, )


stock_season_date_table = Table(
    'stock_season_date', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('date', Date, unique=True, nullable=False))


mapper(StockSeasonDate, stock_season_date_table,
       properties={
           'id': stock_season_date_table.c.id,
           'date': stock_season_date_table.c.date})

StockSeasonDate.__table__ = stock_season_date_table
