from sqlalchemy import Table, Column
from sqlalchemy.orm import mapper
from sqlalchemy.types import Integer, String

from stockprophet.db import metadata


class StockType(object):
    __table__ = None

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return "<StockType('%s')>" % (self.name, )


stock_type_table = Table(
    'stock_type', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(32), unique=True, nullable=False))

mapper(StockType, stock_type_table,
       properties={
           'id': stock_type_table.c.id,
           'name': stock_type_table.c.name})

StockType.__table__ = stock_type_table
