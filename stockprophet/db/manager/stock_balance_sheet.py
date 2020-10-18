import datetime

from sqlalchemy.orm.session import Session
from sqlalchemy import select, update, delete, asc, desc

from stockprophet.db.manager import db_api
from stockprophet.db.model.stock import Stock
from stockprophet.db.model.stock_type import StockType
from stockprophet.db.model.stock_season_date import StockSeasonDate
from stockprophet.db.model.stock_balance_sheet import StockBalanceSheet
from stockprophet.utils import get_logger


logger = get_logger(__name__)


@db_api
def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立每季資產負債表"""
    result = False
    if data_list is None:
        data_list = []

    if data_list:
        s.execute(StockBalanceSheet.__table__.insert(), data_list)
        s.commit()
        result = True

    return result


@db_api
def read_api(s: Session, code: str,
             start_date: datetime.date = None, end_date: datetime.date = None,
             order_desc: bool = False, limit: int = 100) -> list:
    """依據股票代號查詢每季資產負債表資料"""
    result = []

    b_obj = StockBalanceSheet.__table__.c
    s_obj = Stock.__table__.c
    d_obj = StockSeasonDate.__table__.c
    stmt = select([
        b_obj.id, d_obj.date, s_obj.code, s_obj.name,
        b_obj.intangible_assets, b_obj.total_assets, b_obj.total_liabs, b_obj.short_term_borrowing,
        b_obj.total_current_assets, b_obj.total_non_current_assets, b_obj.total_current_liabs,
        b_obj.total_non_current_liabs, b_obj.accrued_payable, b_obj.other_payable,
        b_obj.capital_reserve, b_obj.common_stocks, b_obj.total_stocks, b_obj.inventories,
        b_obj.prepaid, b_obj.shareholders_net_income]
    ).select_from(
        StockBalanceSheet.__table__.join(
            StockSeasonDate.__table__, d_obj.id == b_obj.stock_date_id
        ).join(
            Stock.__table__, s_obj.id == b_obj.stock_id
        )).where(s_obj.code == code)

    if start_date is not None:
        stmt = stmt.where(d_obj.date >= start_date)
    if end_date is not None:
        stmt = stmt.where(d_obj.date <= end_date)

    if order_desc:
        stmt = stmt.order_by(desc(d_obj.date))
    else:
        stmt = stmt.order_by(asc(d_obj.date))

    if limit != 0:
        stmt = stmt.limit(limit)
    for r in s.execute(stmt):
        result.append({
            'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3],
            'intangible_assets': r[4], 'total_assets': r[5], 'total_liabs': r[6],
            'short_term_borrowing': r[7], 'total_current_assets': r[8], 'total_non_current_assets': r[9],
            'total_current_liabs': r[10], 'total_non_current_liabs': r[11], 'accrued_payable': r[12],
            'other_payable': r[13], 'capital_reserve': r[14], 'common_stocks': r[15],
            'total_stocks': r[16], 'inventories': r[17], 'prepaid': r[18], 'shareholders_net_income': r[19]})

    return result


@db_api
def readall_api(s: Session, type_s: str, stock_date: datetime.date = None) -> list:
    """根據日期找出當季所有個股的資產負債表資料"""
    if stock_date is None:
        stock_date = datetime.datetime.today().date()
    result = []

    s_obj = Stock.__table__.c
    b_obj = StockBalanceSheet.__table__.c
    d_obj = StockSeasonDate.__table__.c
    t_obj = StockType.__table__.c
    subquery = select([t_obj.id])\
        .where(t_obj.name == type_s).limit(1)
    stmt = select([
        b_obj.id, d_obj.date, s_obj.code, s_obj.name,
        b_obj.intangible_assets, b_obj.total_assets, b_obj.total_liabs, b_obj.short_term_borrowing,
        b_obj.total_current_assets, b_obj.total_non_current_assets, b_obj.total_current_liabs,
        b_obj.total_non_current_liabs, b_obj.accrued_payable, b_obj.other_payable,
        b_obj.capital_reserve, b_obj.common_stocks, b_obj.total_stocks, b_obj.inventories,
        b_obj.prepaid, b_obj.shareholders_net_income]
    ).select_from(
        StockBalanceSheet.__table__.join(
            StockSeasonDate.__table__, d_obj.id == b_obj.stock_date_id
        ).join(
            Stock.__table__, s_obj.id == b_obj.stock_id
        )
    ).where(
        d_obj.date == stock_date
    ).where(s_obj.stock_type_id == subquery).order_by(asc(b_obj.stock_id))

    for r in s.execute(stmt):
        result.append({
            'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3],
            'intangible_assets': r[4], 'total_assets': r[5], 'total_liabs': r[6],
            'short_term_borrowing': r[7], 'total_current_assets': r[8], 'total_non_current_assets': r[9],
            'total_current_liabs': r[10], 'total_non_current_liabs': r[11], 'accrued_payable': r[12],
            'other_payable': r[13], 'capital_reserve': r[14], 'common_stocks': r[15],
            'total_stocks': r[16], 'inventories': r[17], 'prepaid': r[18], 'shareholders_net_income': r[19]})

    return result


@db_api
def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新資產負債表資料"""
    result = False
    if update_data is None:
        update_data = {}

    if update_data:
        obj = StockBalanceSheet.__table__.c
        stmt = update(StockBalanceSheet.__table__)\
            .where(obj.id == oid).values(update_data)
        s.execute(stmt)
        s.commit()
        result = True

    return result


@db_api
def delete_api(s: Session, oid: int) -> bool:
    """依據id刪除資產負債表資料"""
    obj = StockBalanceSheet.__table__.c
    stmt = delete(StockBalanceSheet.__table__).where(obj.id == oid)
    s.execute(stmt)
    s.commit()
    return True
