import datetime

from sqlalchemy.orm.session import Session
from sqlalchemy import select, update, delete, asc

from stockprophet.db.manager import db_api
from stockprophet.db.model.stock import Stock
from stockprophet.db.model.stock_type import StockType
from stockprophet.db.model.stock_daily_date import StockDailyDate
from stockprophet.db.model.stock_daily_history import StockDailyHistory
from stockprophet.utils import get_logger


logger = get_logger(__name__)


@db_api
def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立股市日期"""
    result = False
    if data_list is None:
        data_list = []

    if data_list:
        s.execute(StockDailyDate.__table__.insert(), data_list)
        s.commit()
        result = True
    return result


@db_api
def read_api(s: Session, date: datetime.date) -> list:
    """依據名稱查詢股市日期"""
    result = []
    obj = StockDailyDate.__table__.c
    stmt = select([obj.id, obj.date]).where(obj.date == date).limit(1)
    for r in s.execute(stmt):
        result.append({'id': r[0], 'date': r[1]})

    return result


@db_api
def readall_api(s: Session, type_s: str) -> list:
    """根據股市類型查詢所有股市日期(不重複)"""
    s_obj = Stock.__table__.c
    h_obj = StockDailyHistory.__table__.c
    d_obj = StockDailyDate.__table__.c
    t_obj = StockType.__table__.c
    subquery = select([t_obj.id])\
        .where(t_obj.name == type_s).limit(1)
    stmt = select(
        [d_obj.id, d_obj.date]
    ).select_from(
        StockDailyHistory.__table__.join(
            StockDailyDate.__table__, d_obj.id == h_obj.stock_date_id
        ).join(
            Stock.__table__, s_obj.id == h_obj.stock_id
        )
    ).order_by(asc(d_obj.id)).where(s_obj.stock_type_id == subquery).distinct()
    result = [{'id': r[0], 'date': r[1]} for r in s.execute(stmt)]

    return result


@db_api
def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新股市日期"""
    result = False
    if update_data is None:
        update_data = {}

    if update_data:
        obj = StockDailyDate.__table__.c
        stmt = update(StockDailyDate.__table__)\
            .where(obj.id == oid).values(update_data)
        s.execute(stmt)
        s.commit()
        result = True

    return result


@db_api
def delete_api(s: Session, oid: int) -> bool:
    """依據id刪除股市日期"""
    obj = StockDailyDate.__table__.c
    stmt = delete(StockDailyDate.__table__).where(obj.id == oid)
    s.execute(stmt)
    s.commit()
    return True
