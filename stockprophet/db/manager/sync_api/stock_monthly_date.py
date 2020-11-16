import datetime

from sqlalchemy.orm.session import Session
from sqlalchemy import exc, insert, select, update, delete, asc

from stockprophet.db.model.stock import stock_table
from stockprophet.db.model.stock_type import stock_type_table
from stockprophet.db.model.stock_monthly_date import stock_monthly_date_table
from stockprophet.db.model.stock_monthly_history import stock_monthly_history_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立股市日期"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(stock_monthly_date_table).values(data_list)
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
    """依據名稱查詢股市日期"""
    result = []
    query = select(
        [stock_monthly_date_table.c.id, stock_monthly_date_table.c.date]
    ).where(stock_monthly_date_table.c.date == date).limit(1)
    try:
        for r in s.execute(query):
            result.append({'id': r[0], 'date': r[1]})
    except exc.SQLAlchemyError as e:
        s.rollback()
        s.close()
        logger.error(str(e))
    finally:
        return result


def readall_api(s: Session, type_s: str) -> list:
    """根據股市類型查詢所有股市日期(不重複)"""
    result = []

    subquery = select([stock_type_table.c.id]).where(stock_type_table.c.name == type_s).limit(1)
    query = select(
        [stock_monthly_date_table.c.id, stock_monthly_date_table.c.date]
    ).select_from(
        stock_monthly_history_table.join(
            stock_monthly_date_table, stock_monthly_date_table.id == stock_monthly_history_table.c.stock_date_id
        ).join(
            stock_table, stock_table.c.id == stock_monthly_history_table.c.stock_id
        )
    ).order_by(asc(stock_monthly_date_table.c.id)).where(stock_table.c.stock_type_id == subquery).distinct()

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
    """依據id更新股市日期"""
    result = False
    if not update_data:
        return result

    query = update(stock_monthly_date_table).where(stock_monthly_date_table.c.id == oid).values(update_data)
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
    """依據id刪除股市日期"""
    result = False
    query = delete(stock_monthly_date_table).where(stock_monthly_date_table.c.id == oid)
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
