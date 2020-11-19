import datetime

from sqlalchemy import Table, Column, ForeignKey, UniqueConstraint
from sqlalchemy.orm import mapper, relationship
from sqlalchemy.types import Integer, Float, String, Date

from stockprophet.db import metadata
from stockprophet.db.model.stock import Stock


class StockCapitalReduction(object):
    __table__ = None

    def __init__(self, old_price: float, new_price: float, reason: str,
                 new_kilo_stock: float, give_back_per_stock: float,
                 stop_tran_date: datetime.date, revive_date: datetime.date):
        self.old_price = old_price
        self.new_price = new_price
        self.reason = reason
        self.new_kilo_stock = new_kilo_stock
        self.give_back_per_stock = give_back_per_stock
        self.stop_tran_date = stop_tran_date
        self.revive_date = revive_date

    def __repr__(self):
        return '<StockCapitalReduction>'


stock_capital_reduction_table = Table(
    'stock_capital_reduction', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('old_price', Float, nullable=False),
    Column('new_price', Float, nullable=False),
    Column('reason', String(32), nullable=False),
    Column('new_kilo_stock', Float, nullable=False),
    Column('give_back_per_stock', Float, nullable=False),
    Column('stop_tran_date', Date, nullable=False),
    Column('revive_date', Date, nullable=False),
    Column('stock_id', ForeignKey('stock.id'), nullable=False),
    UniqueConstraint('stock_id', 'revive_date', name='stock_capital_reduction_uc'))

mapper(StockCapitalReduction, stock_capital_reduction_table,
       properties={
           'id': stock_capital_reduction_table.c.id,
           'old_price': stock_capital_reduction_table.c.old_price,
           'new_price': stock_capital_reduction_table.c.new_price,
           'reason': stock_capital_reduction_table.c.reason,
           'new_kilo_stock': stock_capital_reduction_table.c.new_kilo_stock,
           'give_back_per_stock': stock_capital_reduction_table.c.give_back_per_stock,
           'stop_tran_date': stock_capital_reduction_table.c.stop_tran_date,
           'revive_date': stock_capital_reduction_table.c.revive_date,
           'stock': relationship(Stock)})

StockCapitalReduction.__table__ = stock_capital_reduction_table
