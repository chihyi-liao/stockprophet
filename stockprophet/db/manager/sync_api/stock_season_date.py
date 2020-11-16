import datetime

from sqlalchemy.orm.session import Session
from sqlalchemy import exc, insert, select, update, delete, asc

from stockprophet.db.model.stock_season_date import stock_season_date_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立股市日期"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(stock_season_date_table).values(data_list)
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


def read_api(s: Session, date: datetime.date) -> list:
    """查詢季報表日期"""
    result = []

    query = select(
        [stock_season_date_table.c.id, stock_season_date_table.c.date]
    ).where(stock_season_date_table.c.date == date).limit(1)

    try:
        for r in s.execute(query):
            result.append({'id': r[0], 'date': r[1]})
    except exc.SQLAlchemyError as e:
        s.rollback()
        s.close()
        logger.error(str(e))
    finally:
        return result


def readall_api(s: Session) -> list:
    """查詢所有季報表日期"""
    result = []

    query = select(
        [stock_season_date_table.c.id, stock_season_date_table.c.date]
    ).order_by(asc(stock_season_date_table.c.date))

    try:
        for r in s.execute(query):
            result.append({'id': r[0], 'date': r[1]})
    except exc.SQLAlchemyError as e:
        s.rollback()
        s.close()
        logger.error(str(e))
    finally:
        return result


def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新季報表日期"""
    result = False
    if not update_data:
        return result

    query = update(stock_season_date_table).where(stock_season_date_table.c.id == oid).values(update_data)
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
    """依據id刪除季報表日期"""
    result = False
    query = delete(stock_season_date_table).where(stock_season_date_table.c.id == oid)
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
