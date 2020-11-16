from sqlalchemy import Table, Column, ForeignKey, UniqueConstraint
from sqlalchemy.orm import mapper, relationship
from sqlalchemy.types import Boolean, Integer, String

from stockprophet_db import metadata
from stockprophet_db.model.stock_type import StockType
from stockprophet_db.model.stock_category import StockCategory


class Stock(object):
    __table__ = None

    def __init__(self, code: str, name: str, is_alive: bool,
                 stock_type: StockType, stock_category: StockCategory):
        self.code = code
        self.name = name
        self.is_alive = is_alive
        self.stock_type = stock_type
        self.stock_category = stock_category

    def __repr__(self):
        return "<Stock('%s', '%s', '%s', '%s', '%s')>" % (
            self.code, self.name, self.is_alive, self.stock_type, self.stock_category)


stock_table = Table(
    'stock', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('code', String(32), nullable=False),
    Column('name', String(32), nullable=False),
    Column('is_alive', Boolean, nullable=False),
    Column('stock_type_id', ForeignKey('stock_type.id'), nullable=False),
    Column('stock_category_id', ForeignKey('stock_category.id'), nullable=False),
    UniqueConstraint('code', 'stock_type_id', name='stock_uc'))

mapper(Stock, stock_table,
       properties={
           'id': stock_table.c.id,
           'code': stock_table.c.code,
           'name': stock_table.c.name,
           'is_alive': stock_table.c.is_alive,
           'stock_type': relationship(StockType),
           'stock_category': relationship(StockCategory)})

Stock.__table__ = stock_table
