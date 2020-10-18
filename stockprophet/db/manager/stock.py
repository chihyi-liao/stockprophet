from sqlalchemy.orm.session import Session
from sqlalchemy import select, update, delete

from stockprophet.db.manager import db_api
from stockprophet.db.model.stock import Stock
from stockprophet.db.model.stock_category import StockCategory
from stockprophet.db.model.stock_type import StockType
from stockprophet.utils import get_logger


logger = get_logger(__name__)


@db_api
def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立股市型態"""
    result = False
    if data_list is None:
        data_list = []

    if data_list:
        s.execute(Stock.__table__.insert(), data_list)
        s.commit()
        result = True

    return result


@db_api
def read_api(s: Session, type_s: str, code: str) -> list:
    """依據代號查詢股市"""
    result = []

    obj = Stock.__table__.c
    t_obj = StockType.__table__.c
    subquery = select([t_obj.id])\
        .where(t_obj.name == type_s).limit(1)

    stmt = select(
        [obj.id, obj.code, obj.name, obj.is_alive,
         obj.stock_type_id, obj.stock_category_id]
    ).where(
        obj.code == code
    ).where(
        obj.stock_type_id == subquery
    ).limit(1)
    for r in s.execute(stmt):
        result.append({
            'id': r[0], 'code': r[1], 'name': r[2], 'is_alive': r[3],
            'stock_type_id': r[4], 'stock_category_id': r[5]})

    return result


@db_api
def readall_api(s: Session, type_s: str = '', category_s: str = '', is_alive: bool = None) -> list:
    """查詢所有股市型態"""
    obj = Stock.__table__.c
    t_obj = StockType.__table__.c
    c_obj = StockCategory.__table__.c
    stmt = select([
        obj.id, obj.code, obj.name, obj.is_alive,
        obj.stock_type_id, obj.stock_category_id]
    ).select_from(
        Stock.__table__.join(
            StockType.__table__, t_obj.id == obj.stock_type_id
        ).join(
            StockCategory.__table__, c_obj.id == obj.stock_category_id
        )
    )
    if type_s:
        stmt = stmt.where(t_obj.name == type_s)
    if category_s:
        stmt = stmt.where(c_obj.name == category_s)
    if is_alive is not None:
        stmt = stmt.where(obj.is_alive == is_alive)

    result = [{
        'id': r[0], 'code': r[1], 'name': r[2], 'is_alive': r[3],
        'stock_type_id': r[4], 'stock_category_id': r[5]} for r in s.execute(stmt)]
    return result


@db_api
def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新股市"""
    result = False
    if update_data is None:
        update_data = {}

    if update_data:
        obj = Stock.__table__.c
        stmt = update(Stock.__table__).where(obj.id == oid).values(update_data)
        s.execute(stmt)
        s.commit()
        result = True

    return result


@db_api
def delete_api(s: Session, oid: int) -> bool:
    """依據id刪除股市"""
    obj = Stock.__table__.c
    stmt = delete(Stock.__table__).where(obj.id == oid)
    s.execute(stmt)
    s.commit()
    return True
