import datetime

from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.orm import mapper, relationship
from sqlalchemy.types import Integer, Date

from stockprophet.db.model.stock import Stock
from stockprophet.db import metadata


class StockMetadata(object):
    __table__ = None

    def __init__(self,
                 daily_history_create_date: datetime.date,
                 daily_history_update_date: datetime.date,
                 weekly_history_create_date: datetime.date,
                 weekly_history_update_date: datetime.date,
                 monthly_history_create_date: datetime.date,
                 monthly_history_update_date: datetime.date,
                 income_create_date: datetime.date,
                 income_update_date: datetime.date,
                 balance_create_date: datetime.date,
                 balance_update_date: datetime.date):
        self.daily_history_create_date = daily_history_create_date
        self.daily_history_update_date = daily_history_update_date
        self.weekly_history_create_date = weekly_history_create_date
        self.weekly_history_update_date = weekly_history_update_date
        self.monthly_history_create_date = monthly_history_create_date
        self.monthly_history_update_date = monthly_history_update_date
        self.income_create_date = income_create_date
        self.income_update_date = income_update_date
        self.balance_create_date = balance_create_date
        self.balance_update_date = balance_update_date

    def __repr__(self):
        return "<StockMetadata>"


stock_metadata_table = Table(
    'stock_metadata', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('daily_history_create_date', Date, nullable=True),
    Column('daily_history_update_date', Date, nullable=True),
    Column('weekly_history_create_date', Date, nullable=True),
    Column('weekly_history_update_date', Date, nullable=True),
    Column('monthly_history_create_date', Date, nullable=True),
    Column('monthly_history_update_date', Date, nullable=True),
    Column('income_create_date', Date, nullable=True),
    Column('income_update_date', Date, nullable=True),
    Column('balance_create_date', Date, nullable=True),
    Column('balance_update_date', Date, nullable=True),
    Column('stock_id', ForeignKey('stock.id'), nullable=False, primary_key=True))

mapper(StockMetadata, stock_metadata_table,
       properties={
           'id': stock_metadata_table.c.id,
           'daily_history_create_date': stock_metadata_table.c.daily_history_create_date,
           'daily_history_update_date': stock_metadata_table.c.daily_history_update_date,
           'weekly_history_create_date': stock_metadata_table.c.weekly_history_create_date,
           'weekly_history_update_date': stock_metadata_table.c.weekly_history_update_date,
           'monthly_history_create_date': stock_metadata_table.c.monthly_history_create_date,
           'monthly_history_update_date': stock_metadata_table.c.monthly_history_update_date,
           'income_create_date': stock_metadata_table.c.income_create_date,
           'income_update_date': stock_metadata_table.c.income_update_date,
           'balance_create_date': stock_metadata_table.c.balance_create_date,
           'balance_update_date': stock_metadata_table.c.balance_update_date,
           'stock': relationship(Stock, uselist=False)})

StockMetadata.__table__ = stock_metadata_table
