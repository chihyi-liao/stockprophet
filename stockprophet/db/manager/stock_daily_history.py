import datetime

from sqlalchemy.orm.session import Session
from sqlalchemy import select, update, delete, asc, desc

from stockprophet.db.manager import db_api
from stockprophet.db.model.stock import Stock
from stockprophet.db.model.stock_type import StockType
from stockprophet.db.model.stock_daily_date import StockDailyDate
from stockprophet.db.model.stock_daily_history import StockDailyHistory
from stockprophet.utils import get_logger


logger = get_logger(__name__)


@db_api
def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立每日股市歷史資料"""
    result = False
    if data_list is None:
        data_list = []

    if data_list:
        s.execute(StockDailyHistory.__table__.insert(), data_list)
        s.commit()
        result = True

    return result


@db_api
def read_api(s: Session, code: str,
             start_date: datetime.date = None, end_date: datetime.date = None,
             order_desc: bool = False, limit: int = 100) -> list:
    """依據股票代號查詢每日股市歷史資料"""
    result = []
    h_obj = StockDailyHistory.__table__.c
    s_obj = Stock.__table__.c
    d_obj = StockDailyDate.__table__.c
    stmt = select([
        h_obj.id, d_obj.date, s_obj.code, s_obj.name,
        h_obj.op, h_obj.hi, h_obj.lo, h_obj.co, h_obj.vol, h_obj.val,
        h_obj.ch]
    ).select_from(
        StockDailyHistory.__table__.join(
            StockDailyDate.__table__, d_obj.id == h_obj.stock_date_id
        ).join(
            Stock.__table__, s_obj.id == h_obj.stock_id
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
            'op': r[4], 'hi': r[5], 'lo': r[6], 'co': r[7], 'vol': r[8], 'val': r[9], 'ch': r[10]})
    return result


@db_api
def readall_api(s: Session, type_s: str, stock_date: datetime.date = None) -> list:
    """根據日期找出當日所有個股的每日股市歷史資料"""
    if stock_date is None:
        stock_date = datetime.datetime.today().date()
    result = []
    s_obj = Stock.__table__.c
    h_obj = StockDailyHistory.__table__.c
    d_obj = StockDailyDate.__table__.c
    t_obj = StockType.__table__.c
    subquery = select([t_obj.id]).where(t_obj.name == type_s).limit(1)
    stmt = select([
        h_obj.id, d_obj.date, s_obj.code, s_obj.name, h_obj.op,
        h_obj.hi, h_obj.lo, h_obj.co, h_obj.vol, h_obj.val, h_obj.ch,
        h_obj.stock_id, h_obj.stock_date_id]
    ).select_from(
        StockDailyHistory.__table__.join(
            StockDailyDate.__table__, d_obj.id == h_obj.stock_date_id
        ).join(
            Stock.__table__, s_obj.id == h_obj.stock_id
        )
    ).where(d_obj.date == stock_date).where(s_obj.stock_type_id == subquery).order_by(asc(h_obj.stock_id))

    for r in s.execute(stmt):
        result.append({
            'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3],
            'op': r[4], 'hi': r[5], 'lo': r[6], 'co': r[7], 'vol': r[8], 'val': r[9], 'ch': r[10]})
    return result


@db_api
def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新每日股市歷史資料"""
    result = False
    if update_data is None:
        update_data = {}

    if update_data:
        obj = StockDailyHistory.__table__.c
        stmt = update(StockDailyHistory.__table__).where(obj.id == oid).values(update_data)
        s.execute(stmt)
        s.commit()
        result = True

    return result


@db_api
def delete_api(s: Session, oid: int) -> bool:
    """依據id刪除每日股市歷史資料"""
    obj = StockDailyHistory.__table__.c
    stmt = delete(StockDailyHistory.__table__).where(obj.id == oid)
    s.execute(stmt)
    s.commit()
    return True
