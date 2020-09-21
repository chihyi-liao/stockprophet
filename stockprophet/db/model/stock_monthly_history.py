from sqlalchemy import Table, Column, ForeignKey, UniqueConstraint
from sqlalchemy.orm import mapper, relationship
from sqlalchemy.types import BigInteger, Integer, Float

from stockprophet.db import metadata
from stockprophet.db.model.stock import Stock
from stockprophet.db.model.stock_monthly_date import StockMonthlyDate


class StockMonthlyHistory(object):
    __table__ = None

    def __init__(self, op: float, hi: float, lo: float, co: float,
                 vol: int, val: int, ch: float, stock: Stock, stock_date: StockMonthlyDate):
        self.op = op
        self.hi = hi
        self.lo = lo
        self.co = co
        self.vol = vol
        self.val = val
        self.ch = ch
        self.stock = stock
        self.stock_date = stock_date

    def __repr__(self):
        return "<StockMonthlyHistory('%s', '%s', '%s', '%s', '%s')>" % (
            self.stock.name, self.op, self.hi, self.lo, self.co)


stock_monthly_history_table = Table(
    'stock_monthly_history', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('op', Float(precision=8), nullable=True),
    Column('hi', Float(precision=8), nullable=True),
    Column('lo', Float(precision=8), nullable=True),
    Column('co', Float(precision=8), nullable=True),
    Column('vol', BigInteger, nullable=True),
    Column('val', BigInteger, nullable=True),
    Column('ch', Float(precision=8), nullable=True),
    Column('stock_id', ForeignKey('stock.id'), nullable=False),
    Column('stock_date_id', ForeignKey('stock_monthly_date.id'), nullable=False),
    UniqueConstraint('stock_id', 'stock_date_id', name='stock_monthly_history_uc'))

mapper(StockMonthlyHistory, stock_monthly_history_table,
       properties={
           'id': stock_monthly_history_table.c.id,
           'op': stock_monthly_history_table.c.op,
           'hi': stock_monthly_history_table.c.hi,
           'lo': stock_monthly_history_table.c.lo,
           'co': stock_monthly_history_table.c.co,
           'vol': stock_monthly_history_table.c.vol,
           'val': stock_monthly_history_table.c.val,
           'ch': stock_monthly_history_table.c.ch,
           'stock': relationship(Stock),
           'stock_monthly_date': relationship(StockMonthlyDate)})

StockMonthlyHistory.__table__ = stock_monthly_history_table
