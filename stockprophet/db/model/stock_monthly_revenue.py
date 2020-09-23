from sqlalchemy import Table, Column, ForeignKey, UniqueConstraint
from sqlalchemy.orm import mapper, relationship
from sqlalchemy.types import Integer, BigInteger, String

from stockprophet.db import metadata
from stockprophet.db.model.stock import Stock
from stockprophet.db.model.stock_monthly_date import StockMonthlyDate


class StockMonthlyRevenue(object):
    __table__ = None

    def __init__(self, revenue: int, note: str = ''):
        self.revenue = revenue
        self.note = note

    def __repr__(self):
        return '<StockMonthlyRevenue>'


stock_monthly_revenue_table = Table(
    'stock_monthly_revenue', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('revenue', BigInteger, nullable=False),
    Column('note', String(500), nullable=True),
    Column('stock_id', ForeignKey('stock.id'), nullable=False),
    Column('stock_date_id', ForeignKey('stock_monthly_date.id'), nullable=False),
    UniqueConstraint('stock_id', 'stock_date_id', name='stock_monthly_revenue_uc'))

mapper(StockMonthlyRevenue, stock_monthly_revenue_table,
       properties={
           'id': stock_monthly_revenue_table.c.id,
           'revenue': stock_monthly_revenue_table.c.revenue,
           'note': stock_monthly_revenue_table.c.note,
           'stock': relationship(Stock),
           'stock_monthly_date': relationship(StockMonthlyDate)})

StockMonthlyRevenue.__table__ = stock_monthly_revenue_table
