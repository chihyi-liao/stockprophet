import datetime

from sqlalchemy.orm.session import Session
from sqlalchemy import select, update, delete, asc

from stockprophet.db.manager import db_api
from stockprophet.db.model.stock_season_date import StockSeasonDate
from stockprophet.utils import get_logger


logger = get_logger(__name__)


@db_api
def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立股市日期"""
    result = False
    if data_list is None:
        data_list = []

    if data_list:
        s.execute(StockSeasonDate.__table__.insert(), data_list)
        s.commit()
        result = True

    return result


@db_api
def read_api(s: Session, date: datetime.date) -> list:
    """查詢季報表日期"""
    result = []
    obj = StockSeasonDate.__table__.c
    stmt = select([obj.id, obj.date]).where(obj.date == date).limit(1)
    for r in s.execute(stmt):
        result.append({'id': r[0], 'date': r[1]})

    return result


@db_api
def readall_api(s: Session) -> list:
    """查詢所有季報表日期"""
    obj = StockSeasonDate.__table__.c
    stmt = select([obj.id, obj.date]).order_by(asc(obj.id))
    result = [{'id': r[0], 'date': r[1]} for r in s.execute(stmt)]
    return result


@db_api
def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新季報表日期"""
    result = False
    if update_data is None:
        update_data = {}

    if update_data:
        obj = StockSeasonDate.__table__.c
        stmt = update(StockSeasonDate.__table__).where(obj.id == oid).values(update_data)
        s.execute(stmt)
        s.commit()
        result = True

    return result


@db_api
def delete_api(s: Session, oid: int) -> bool:
    """依據id刪除季報表日期"""
    obj = StockSeasonDate.__table__.c
    stmt = delete(StockSeasonDate.__table__).where(obj.id == oid)
    s.execute(stmt)
    s.commit()
    return True
