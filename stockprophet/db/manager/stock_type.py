from sqlalchemy.orm.session import Session
from sqlalchemy import exc, select, update, delete

from stockprophet.db.model.stock_type import StockType
from stockprophet.utils import get_logger


logger = get_logger(__name__)


def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立股市型態"""
    result = False
    if data_list is None:
        data_list = []
    try:
        if data_list:
            s.execute(StockType.__table__.insert(), data_list)
            s.commit()
            result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


def read_api(s: Session, name: str) -> list:
    """依據名稱查詢股市型態"""
    result = []
    try:
        obj = StockType.__table__.c
        stmt = select([obj.id, obj.name])\
            .where(obj.name == name).limit(1)
        for r in s.execute(stmt):
            result.append({'id': r[0], 'name': r[1]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


def readall_api(s: Session) -> list:
    """查詢所有股市型態"""
    result = []
    try:
        obj = StockType.__table__.c
        stmt = select([obj.id, obj.name])
        result = [{'id': r[0], 'name': r[1]} for r in s.execute(stmt)]
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新股市型態"""
    result = False
    if update_data is None:
        update_data = {}
    try:
        if update_data:
            obj = StockType.__table__.c
            stmt = update(StockType.__table__)\
                .where(obj.id == oid).values(update_data)
            s.execute(stmt)
            s.commit()
            result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


def delete_api(s: Session, oid: int) -> bool:
    """依據id刪除股市型態"""
    result = False
    try:
        obj = StockType.__table__.c
        stmt = delete(StockType.__table__).where(obj.id == oid)
        s.execute(stmt)
        s.commit()
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result
