from sqlalchemy.orm.session import Session
from sqlalchemy import select, update, delete, asc

from stockprophet.db.manager import db_api
from stockprophet.db.model.stock_type import StockType
from stockprophet.db.model.stock import Stock
from stockprophet.db.model.stock_metadata import StockMetadata
from stockprophet.utils import get_logger


logger = get_logger(__name__)


@db_api
def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立metadata資料"""
    result = False
    if data_list is None:
        data_list = []

    if data_list:
        s.execute(StockMetadata.__table__.insert(), data_list)
        s.commit()
        result = True

    return result


@db_api
def read_api(s: Session, code: str) -> list:
    """依據股票代號查詢metadata"""
    result = []
    m_obj = StockMetadata.__table__.c
    s_obj = Stock.__table__.c
    stmt = select([
        m_obj.id, s_obj.code, s_obj.name,
        m_obj.daily_history_create_date, m_obj.daily_history_update_date,
        m_obj.weekly_history_create_date, m_obj.weekly_history_update_date,
        m_obj.monthly_history_create_date, m_obj.monthly_history_update_date,
        m_obj.income_create_date, m_obj.income_update_date,
        m_obj.balance_create_date, m_obj.balance_update_date]
    ).select_from(
        StockMetadata.__table__.join(
            Stock.__table__, s_obj.id == m_obj.stock_id
        )
    ).where(s_obj.code == code).limit(1)

    for r in s.execute(stmt):
        result.append({
            'id': r[0], 'code': r[1], 'name': r[2],
            'daily_history_create_date': r[3], 'daily_history_update_date': r[4],
            'weekly_history_create_date': r[5], 'weekly_history_update_date': r[6],
            'monthly_history_create_date': r[7], 'monthly_history_update_date': r[8],
            'income_create_date': r[9], 'income_update_date': r[10],
            'balance_create_date': r[11], 'balance_update_date': r[12]})

    return result


@db_api
def readall_api(s: Session, type_s: str) -> list:
    """查詢所有個股的metadata資料"""
    result = []
    s_obj = Stock.__table__.c
    m_obj = StockMetadata.__table__.c
    t_obj = StockType.__table__.c
    subquery = select([t_obj.id]).where(t_obj.name == type_s).limit(1)
    stmt = select([
        m_obj.id, s_obj.code, s_obj.name,
        m_obj.daily_history_create_date, m_obj.daily_history_update_date,
        m_obj.weekly_history_create_date, m_obj.weekly_history_update_date,
        m_obj.monthly_history_create_date, m_obj.monthly_history_update_date,
        m_obj.income_create_date, m_obj.income_update_date,
        m_obj.balance_create_date, m_obj.balance_update_date]
    ).select_from(
        StockMetadata.__table__.join(
            Stock.__table__, s_obj.id == m_obj.stock_id
        )
    ).where(s_obj.stock_type_id == subquery).order_by(asc(m_obj.code))

    for r in s.execute(stmt):
        result.append({
            'id': r[0], 'code': r[1], 'name': r[2],
            'daily_history_create_date': r[3], 'daily_history_update_date': r[4],
            'weekly_history_create_date': r[5], 'weekly_history_update_date': r[6],
            'monthly_history_create_date': r[7], 'monthly_history_update_date': r[8],
            'income_create_date': r[9], 'income_update_date': r[10],
            'balance_create_date': r[11], 'balance_update_date': r[12]})

    return result


@db_api
def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新metadata資料"""
    result = False
    if update_data is None:
        update_data = {}

    if update_data:
        obj = StockMetadata.__table__.c
        stmt = update(StockMetadata.__table__).where(obj.id == oid).values(update_data)
        s.execute(stmt)
        s.commit()
        result = True

    return result


@db_api
def delete_api(s: Session, oid: int) -> bool:
    """依據id刪除metadata資料"""
    obj = StockMetadata.__table__.c
    stmt = delete(StockMetadata.__table__).where(obj.id == oid)
    s.execute(stmt)
    s.commit()
    return True
