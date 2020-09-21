from sqlalchemy import Table, Column
from sqlalchemy.orm import mapper
from sqlalchemy.types import Integer, String

from stockprophet.db import metadata


class StockCategory(object):
    __table__ = None

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return "<StockCategory('%s')>" % (self.name, )


stock_category_table = Table(
    'stock_category', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(32), unique=True, nullable=False))

mapper(StockCategory, stock_category_table,
       properties={
           'id': stock_category_table.c.id,
           'name': stock_category_table.c.name})

StockCategory.__table__ = stock_category_table
