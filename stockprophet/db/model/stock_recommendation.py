from sqlalchemy import Table, Column, ForeignKey, UniqueConstraint
from sqlalchemy.orm import mapper, relationship
from sqlalchemy.types import Integer, String, Float

from stockprophet.db import metadata
from stockprophet.db.model.stock import Stock
from stockprophet.db.model.stock_daily_date import StockDailyDate


class StockRecommendation(object):
    __table__ = None

    def __init__(self, price: float, note: str):
        self.price = price
        self.note = note

    def __repr__(self):
        return '<StockRecommendation>'


stock_recommendation_table = Table(
    'stock_recommendation', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('price', Float, nullable=False),
    Column('note', String(500), nullable=True),
    Column('stock_id', ForeignKey('stock.id'), nullable=False),
    Column('stock_date_id', ForeignKey('stock_daily_date.id'), nullable=False),
    UniqueConstraint('stock_id', 'stock_date_id', name='stock_recommendation_uc'))

mapper(StockRecommendation, stock_recommendation_table,
       properties={
           'id': stock_recommendation_table.c.id,
           'price': stock_recommendation_table.c.price,
           'note': stock_recommendation_table.c.note,
           'stock': relationship(Stock),
           'stock_daily_date': relationship(StockDailyDate)})

StockRecommendation.__table__ = stock_recommendation_table
