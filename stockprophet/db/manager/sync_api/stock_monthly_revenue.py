import datetime

from sqlalchemy.orm.session import Session
from sqlalchemy import exc, insert, select, update, delete, asc, desc

from stockprophet.db.model.stock import stock_table
from stockprophet.db.model.stock_type import stock_type_table
from stockprophet.db.model.stock_monthly_date import stock_monthly_date_table
from stockprophet.db.model.stock_monthly_revenue import stock_monthly_revenue_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立每月營收資料"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(stock_monthly_revenue_table).values(data_list)
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


def read_api(s: Session, code: str,
             start_date: datetime.date = None, end_date: datetime.date = None,
             order_desc: bool = False, limit: int = 100) -> list:
    """依據股票代號查詢每月營收資料"""
    result = []

    query = select([
        stock_monthly_revenue_table.c.id, stock_monthly_date_table.c.date,
        stock_table.c.code, stock_table.c.name,
        stock_monthly_revenue_table.c.revenue, stock_monthly_revenue_table.c.note]
    ).select_from(
        stock_monthly_revenue_table.join(
            stock_monthly_date_table, stock_monthly_date_table.c.id == stock_monthly_revenue_table.c.stock_date_id
        ).join(
            stock_table, stock_table.c.id == stock_monthly_revenue_table.c.stock_id
        )
    ).where(stock_table.c.code == code)

    if start_date is not None:
        query = query.where(stock_monthly_date_table.c.date >= start_date)
    if end_date is not None:
        query = query.where(stock_monthly_date_table.c.date <= end_date)

    if order_desc:
        query = query.order_by(desc(stock_monthly_date_table.c.date))
    else:
        query = query.order_by(asc(stock_monthly_date_table.c.date))

    if limit != 0:
        query = query.limit(limit)

    try:
        for r in s.execute(query):
            result.append({'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3], 'revenue': r[4], 'note': r[5]})
    except exc.SQLAlchemyError as e:
        s.rollback()
        s.close()
        logger.error(str(e))
    finally:
        return result


def readall_api(s: Session, type_s: str, stock_date: datetime.date = None) -> list:
    """根據日期找出當日所有個股的每月營收資料"""
    result = []
    if stock_date is None:
        stock_date = datetime.datetime.today().date()

    subquery = select([stock_type_table.c.id]).where(stock_type_table.c.name == type_s).limit(1)
    query = select([
        stock_monthly_revenue_table.c.id, stock_monthly_date_table.c.date,
        stock_table.c.code, stock_table.c.name,
        stock_monthly_revenue_table.c.revenue, stock_monthly_revenue_table.c.note]
    ).select_from(
        stock_monthly_revenue_table.join(
            stock_monthly_date_table, stock_monthly_date_table.c.id == stock_monthly_revenue_table.c.stock_date_id
        ).join(
            stock_table, stock_table.c.id == stock_monthly_revenue_table.c.stock_id
        )
    ).where(
        stock_monthly_date_table.c.date == stock_date
    ).where(
        stock_table.c.stock_type_id == subquery
    ).order_by(asc(stock_monthly_revenue_table.c.stock_id))

    try:
        for r in s.execute(query):
            result.append({'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3], 'revenue': r[4], 'note': r[5]})
    except exc.SQLAlchemyError as e:
        s.rollback()
        s.close()
        logger.error(str(e))
    finally:
        return result


def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新每月股市歷史資料"""
    result = False
    if not update_data:
        return result

    query = update(stock_monthly_revenue_table).where(stock_monthly_revenue_table.c.id == oid).values(update_data)
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
    """依據id刪除每月股市歷史資料"""
    result = False
    query = delete(stock_monthly_revenue_table).where(stock_monthly_revenue_table.c.id == oid)
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
