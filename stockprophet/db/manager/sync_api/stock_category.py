from sqlalchemy.orm.session import Session
from sqlalchemy import exc, insert, select, update, delete, asc

from stockprophet.db.model.stock_category import stock_category_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立股市類別"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(stock_category_table).values(data_list)
    try:
        s.execute(query)
        s.commit()
        result = True
    except exc.SQLAlchemyError as e:
        s.rollback()
        s.close()
        logger.error(str(e))
    finally:
        return result


def read_api(s: Session, name: str) -> list:
    """依據名稱查詢股市類別"""
    result = []
    query = select(
        [stock_category_table.c.id, stock_category_table.c.name]
    ).where(stock_category_table.c.name == name).limit(1)
    try:
        for r in s.execute(query):
            result.append({'id': r[0], 'name': r[1]})
    except exc.SQLAlchemyError as e:
        s.rollback()
        s.close()
        logger.error(str(e))
    finally:
        return result


def readall_api(s: Session) -> list:
    """查詢所有股市類別"""
    result = []
    query = select([stock_category_table.c.id, stock_category_table.c.name]).order_by(asc(stock_category_table.c.id))
    try:
        for r in s.execute(query):
            result.append({'id': r[0], 'name': r[1]})
    except exc.SQLAlchemyError as e:
        s.rollback()
        s.close()
        logger.error(str(e))
    finally:
        return result


def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新股市類別"""
    result = False
    if not update_data:
        return result

    query = update(stock_category_table).where(stock_category_table.c.id == oid).values(update_data)
    try:
        s.execute(query)
        s.commit()
        result = True
    except exc.SQLAlchemyError as e:
        s.rollback()
        s.close()
        logger.error(str(e))
    finally:
        return result


def delete_api(s: Session, oid: int) -> bool:
    """依據id刪除股市類別"""
    result = False
    query = delete(stock_category_table).where(stock_category_table.c.id == oid)
    try:
        s.execute(query)
        s.commit()
        result = True
    except exc.SQLAlchemyError as e:
        s.rollback()
        s.close()
        logger.error(str(e))
    finally:
        return result
