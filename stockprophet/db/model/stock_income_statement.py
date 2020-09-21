from sqlalchemy import Table, Column, ForeignKey, UniqueConstraint
from sqlalchemy.orm import mapper, relationship
from sqlalchemy.types import BigInteger, Integer, Float

from stockprophet.db import metadata
from stockprophet.db.model.stock import Stock
from stockprophet.db.model.stock_season_date import StockSeasonDate


class StockIncomeStatement(object):
    __table__ = None

    def __init__(self, net_sales: int = None, cost_of_goods_sold: int = None,
                 gross_profit: int = None, operating_expenses: int = None,
                 operating_income: int = None, total_non_op_income_expenses: int = None,
                 pre_tax_income: int = None, income_tax_expense: int = None,
                 net_income: int = None, other_comprehensive_income: int = None,
                 consolidated_net_income: int = None, eps: float = None):
        self.net_sales = net_sales
        self.cost_of_goods_sold = cost_of_goods_sold
        self.gross_profit = gross_profit
        self.operating_expenses = operating_expenses
        self.operating_income = operating_income
        self.total_non_op_income_expenses = total_non_op_income_expenses
        self.pre_tax_income = pre_tax_income
        self.income_tax_expense = income_tax_expense
        self.net_income = net_income
        self.other_comprehensive_income = other_comprehensive_income
        self.consolidated_net_income = consolidated_net_income
        self.eps = eps

    def __repr__(self):
        return '<StockIncomeStatement>'


stock_income_statement_table = Table(
    'stock_income_statement', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('net_sales', BigInteger, nullable=True),  # 營業收入合計
    Column('cost_of_goods_sold', BigInteger, nullable=True),  # 營業成本合計
    Column('gross_profit', BigInteger, nullable=True),  # 營業毛利（毛損）淨額
    Column('operating_expenses', BigInteger, nullable=True),  # 營業費用合計
    Column('operating_income', BigInteger, nullable=True),  # 營業利益（損失）
    Column('total_non_op_income_expenses', BigInteger, nullable=True),  # 營業外收入及支出合計
    Column('pre_tax_income', BigInteger, nullable=True),  # 稅前淨利（淨損）
    Column('income_tax_expense', BigInteger, nullable=True),  # 所得稅費用（利益）合計
    Column('net_income', BigInteger, nullable=True),  # 本期淨利（淨損）
    Column('other_comprehensive_income', BigInteger, nullable=True),  # 其他綜合損益（淨額
    Column('consolidated_net_income', BigInteger, nullable=True),  # 本期綜合損益總額
    Column('eps', Float(precision=8), nullable=True),  # 基本每股盈餘
    Column('stock_id', ForeignKey('stock.id'), nullable=False),
    Column('stock_date_id', ForeignKey('stock_season_date.id'), nullable=False),
    UniqueConstraint('stock_id', 'stock_date_id', name='stock_income_statement_uc'))

mapper(StockIncomeStatement, stock_income_statement_table,
       properties={
           'id': stock_income_statement_table.c.id,
           'net_sales': stock_income_statement_table.c.net_sales,
           'cost_of_goods_sold': stock_income_statement_table.c.cost_of_goods_sold,
           'gross_profit': stock_income_statement_table.c.gross_profit,
           'operating_expenses': stock_income_statement_table.c.operating_expenses,
           'operating_income': stock_income_statement_table.c.operating_income,
           'total_non_op_income_expenses': stock_income_statement_table.c.total_non_op_income_expenses,
           'pre_tax_income': stock_income_statement_table.c.pre_tax_income,
           'income_tax_expense': stock_income_statement_table.c.income_tax_expense,
           'net_income': stock_income_statement_table.c.net_income,
           'other_comprehensive_income': stock_income_statement_table.c.other_comprehensive_income,
           'consolidated_net_income': stock_income_statement_table.c.consolidated_net_income,
           'eps': stock_income_statement_table.c.eps,
           'stock': relationship(Stock),
           'stock_season_date': relationship(StockSeasonDate)})

StockIncomeStatement.__table__ = stock_income_statement_table
