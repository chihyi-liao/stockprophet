from sqlalchemy import Table, Column, ForeignKey, UniqueConstraint
from sqlalchemy.orm import mapper, relationship
from sqlalchemy.types import BigInteger, Integer

from stockprophet.db import metadata
from stockprophet.db.model.stock import Stock
from stockprophet.db.model.stock_season_date import StockSeasonDate


class StockBalanceSheet(object):
    __table__ = None

    def __init__(self, intangible_assets: int = None, total_assets: int = None,
                 total_liabs: int = None, short_term_borrowing: int = None,
                 total_current_assets: int = None, total_non_current_assets: int = None,
                 total_current_liabs: int = None, total_non_current_liabs: int = None,
                 accrued_payable: int = None, other_payable: int = None,
                 capital_reserve: int = None, common_stocks: int = None,
                 total_stocks: int = None, inventories: int = None,
                 prepaid: int = None, shareholders_net_income: int = None):
        self.intangible_assets = intangible_assets
        self.total_assets = total_assets
        self.total_liabs = total_liabs
        self.short_term_borrowing = short_term_borrowing
        self.total_current_assets = total_current_assets
        self.total_non_current_assets = total_non_current_assets
        self.total_current_liabs = total_current_liabs
        self.total_non_current_liabs = total_non_current_liabs
        self.accrued_payable = accrued_payable
        self.other_payable = other_payable
        self.capital_reserve = capital_reserve
        self.common_stocks = common_stocks
        self.total_stocks = total_stocks
        self.inventories = inventories
        self.prepaid = prepaid
        self.shareholders_net_income = shareholders_net_income

    def __repr__(self):
        return '<StockBalanceSheet>'


stock_balance_sheet_table = Table(
    'stock_balance_sheet', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('intangible_assets', BigInteger, nullable=True),  # 無形資產
    Column('total_assets', BigInteger, nullable=True),  # 資產總額
    Column('total_liabs', BigInteger, nullable=True),  # 負債總額
    Column('short_term_borrowing', BigInteger, nullable=True),  # 短期借款
    Column('total_current_assets', BigInteger, nullable=True),  # 流動資產
    Column('total_non_current_assets', BigInteger, nullable=True),  # 非流動資產
    Column('total_current_liabs', BigInteger, nullable=True),  # 流動負債
    Column('total_non_current_liabs', BigInteger, nullable=True),  # 非流動負債
    Column('accrued_payable', BigInteger, nullable=True),  # 應付帳款
    Column('other_payable', BigInteger, nullable=True),  # 其他應付款
    Column('capital_reserve', BigInteger, nullable=True),  # 資本公積
    Column('common_stocks', BigInteger, nullable=True),  # 普通股股本
    Column('total_stocks', BigInteger, nullable=True),  # 股本合計
    Column('inventories', BigInteger, nullable=True),  # 存貨
    Column('prepaid', BigInteger, nullable=True),  # 預付款項
    Column('shareholders_net_income', BigInteger, nullable=True),  # 歸屬於母公司業主之權益合計
    Column('stock_id', ForeignKey('stock.id'), nullable=False),
    Column('stock_date_id', ForeignKey('stock_season_date.id'), nullable=False),
    UniqueConstraint('stock_id', 'stock_date_id', name='stock_balance_sheet_uc'))

mapper(StockBalanceSheet, stock_balance_sheet_table,
       properties={
           'id': stock_balance_sheet_table.c.id,
           'intangible_assets': stock_balance_sheet_table.c.intangible_assets,
           'total_assets': stock_balance_sheet_table.c.total_assets,
           'total_liabs': stock_balance_sheet_table.c.total_liabs,
           'short_term_borrowing': stock_balance_sheet_table.c.short_term_borrowing,
           'total_current_assets': stock_balance_sheet_table.c.total_current_assets,
           'total_non_current_assets': stock_balance_sheet_table.c.total_non_current_assets,
           'total_current_liabs': stock_balance_sheet_table.c.total_current_liabs,
           'total_non_current_liabs': stock_balance_sheet_table.c.total_non_current_liabs,
           'accrued_payable': stock_balance_sheet_table.c.accrued_payable,
           'other_payable': stock_balance_sheet_table.c.other_payable,
           'capital_reserve': stock_balance_sheet_table.c.capital_reserve,
           'common_stocks': stock_balance_sheet_table.c.common_stocks,
           'total_stocks': stock_balance_sheet_table.c.total_stocks,
           'inventories': stock_balance_sheet_table.c.inventories,
           'prepaid': stock_balance_sheet_table.c.prepaid,
           'shareholders_net_income': stock_balance_sheet_table.c.shareholders_net_income,
           'stock': relationship(Stock),
           'stock_season_date': relationship(StockSeasonDate)})

StockBalanceSheet.__table__ = stock_balance_sheet_table
