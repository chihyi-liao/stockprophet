import datetime

from sqlalchemy import Table, Column
from sqlalchemy.orm import mapper
from sqlalchemy.types import Integer, Date

from stockprophet.db import metadata


class StockMetadata(object):
    __table__ = None

    def __init__(self,
                 tse_stock_info_update_date: datetime.date,
                 tse_weekly_history_update_date: datetime.date,
                 tse_monthly_history_update_date: datetime.date,
                 otc_stock_info_update_date: datetime.date,
                 otc_weekly_history_update_date: datetime.date,
                 otc_monthly_history_update_date: datetime.date,
                 mops_income_statement_update_date: datetime.date,
                 mops_balance_sheet_update_date: datetime.date):
        self.tse_stock_info_update_date = tse_stock_info_update_date
        self.tse_weekly_history_update_date = tse_weekly_history_update_date
        self.tse_monthly_history_update_date = tse_monthly_history_update_date
        self.otc_stock_info_update_date = otc_stock_info_update_date
        self.otc_weekly_history_update_date = otc_weekly_history_update_date
        self.otc_monthly_history_update_date = otc_monthly_history_update_date
        self.mops_income_statement_update_date = mops_income_statement_update_date
        self.mops_balance_sheet_update_date = mops_balance_sheet_update_date

    def __repr__(self):
        return "<StockMetadata>"


stock_metadata_table = Table(
    'stock_metadata', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('tse_stock_info_update_date', Date, unique=True, nullable=True),
    Column('tse_weekly_history_update_date', Date, unique=True, nullable=True),
    Column('tse_monthly_history_update_date', Date, unique=True, nullable=True),
    Column('otc_stock_info_update_date', Date, unique=True, nullable=True),
    Column('otc_weekly_history_update_date', Date, unique=True, nullable=True),
    Column('otc_monthly_history_update_date', Date, unique=True, nullable=True),
    Column('mops_income_statement_update_date', Date, unique=True, nullable=True),
    Column('mops_balance_sheet_update_date', Date, unique=True, nullable=True))

mapper(StockMetadata, stock_metadata_table,
       properties={
           'id': stock_metadata_table.c.id,
           'tse_stock_info_update_date': stock_metadata_table.c.tse_stock_info_update_date,
           'tse_weekly_history_update_date': stock_metadata_table.c.tse_weekly_history_update_date,
           'tse_monthly_history_update_date': stock_metadata_table.c.tse_monthly_history_update_date,
           'otc_stock_info_update_date': stock_metadata_table.c.otc_stock_info_update_date,
           'otc_weekly_history_update_date': stock_metadata_table.c.otc_weekly_history_update_date,
           'otc_monthly_history_update_date': stock_metadata_table.c.otc_monthly_history_update_date,
           'mops_income_statement_update_date': stock_metadata_table.c.mops_income_statement_update_date,
           'mops_balance_sheet_update_date': stock_metadata_table.c.mops_balance_sheet_update_date})

StockMetadata.__table__ = stock_metadata_table
