import datetime

from sqlalchemy import Table, Column
from sqlalchemy.orm import mapper
from sqlalchemy.types import Integer, Date

from stockprophet.db import metadata


class StockWeeklyDate(object):
    __table__ = None

    def __init__(self, date: datetime.date):
        self.date = date

    def __repr__(self):
        return "<StockWeeklyDate('%s')>" % (self.date, )


stock_weekly_date_table = Table(
    'stock_weekly_date', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('date', Date, unique=True, nullable=False))


mapper(StockWeeklyDate, stock_weekly_date_table,
       properties={
           'id': stock_weekly_date_table.c.id,
           'date': stock_weekly_date_table.c.date})

StockWeeklyDate.__table__ = stock_weekly_date_table
