import datetime

from databases import Database
from sqlalchemy import exc, insert, select, update, delete, asc, desc

from stockprophet.db.model.stock import stock_table
from stockprophet.db.model.stock_type import stock_type_table
from stockprophet.db.model.stock_daily_date import stock_daily_date_table
from stockprophet.db.model.stock_daily_history import stock_daily_history_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


async def create_api(db: Database, data_list: list = None) -> bool:
    """依據資料清單建立每日股市歷史資料"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(stock_daily_history_table).values(data_list)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def read_api(db: Database, code: str, start_date: datetime.date = None, end_date: datetime.date = None,
                   order_desc: bool = False, limit: int = 100) -> list:
    """依據股票代號查詢每日股市歷史資料"""
    result = []

    query = select([
        stock_daily_history_table.c.id, stock_daily_date_table.c.date,
        stock_table.c.code, stock_table.c.name,
        stock_daily_history_table.c.op, stock_daily_history_table.c.hi,
        stock_daily_history_table.c.lo, stock_daily_history_table.c.co,
        stock_daily_history_table.c.vol, stock_daily_history_table.c.val, stock_daily_history_table.c.ch]
    ).select_from(
        stock_daily_history_table.join(
            stock_daily_date_table, stock_daily_date_table.c.id == stock_daily_history_table.c.stock_date_id
        ).join(
            stock_table, stock_table.c.id == stock_daily_history_table.c.stock_id
        )
    ).where(stock_table.c.code == code)

    if start_date is not None:
        query = query.where(stock_daily_date_table.c.date >= start_date)
    if end_date is not None:
        query = query.where(stock_daily_date_table.c.date <= end_date)

    if order_desc:
        query = query.order_by(desc(stock_daily_date_table.c.date))
    else:
        query = query.order_by(asc(stock_daily_date_table.c.date))

    if limit != 0:
        query = query.limit(limit)

    try:
        for r in await db.fetch_all(query):
            result.append({
                'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3],
                'op': r[4], 'hi': r[5], 'lo': r[6], 'co': r[7], 'vol': r[8], 'val': r[9], 'ch': r[10]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def readall_api(db: Database, type_s: str, stock_date: datetime.date = None) -> list:
    """根據日期找出當日所有個股的每日股市歷史資料"""
    result = []
    if stock_date is None:
        stock_date = datetime.datetime.today().date()

    subquery = select([stock_type_table.c.id]).where(stock_type_table.c.name == type_s).limit(1)
    query = select([
        stock_daily_history_table.c.id, stock_daily_date_table.c.date,
        stock_table.c.code, stock_table.c.name,
        stock_daily_history_table.c.op, stock_daily_history_table.c.hi,
        stock_daily_history_table.c.lo, stock_daily_history_table.c.co,
        stock_daily_history_table.c.vol, stock_daily_history_table.c.val, stock_daily_history_table.c.ch]
    ).select_from(
        stock_daily_history_table.join(
            stock_daily_date_table, stock_daily_date_table.c.id == stock_daily_history_table.c.stock_date_id
        ).join(
            stock_table, stock_table.c.id == stock_daily_history_table.c.stock_id
        )
    ).where(
        stock_daily_date_table.c.date == stock_date
    ).where(
        stock_table.c.stock_type_id == subquery
    ).order_by(asc(stock_daily_history_table.c.stock_id))

    try:
        for r in await db.fetch_all(query):
            result.append({
                'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3],
                'op': r[4], 'hi': r[5], 'lo': r[6], 'co': r[7], 'vol': r[8], 'val': r[9], 'ch': r[10]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def update_api(db: Database, oid: int, update_data: dict = None) -> bool:
    """依據id更新每日股市歷史資料"""
    result = False
    if not update_data:
        return result

    query = update(stock_daily_history_table).where(stock_daily_history_table.c.id == oid).values(update_data)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def delete_api(db: Database, oid: int) -> bool:
    """依據id刪除每日股市歷史資料"""
    result = False
    query = delete(stock_daily_history_table).where(stock_daily_history_table.c.id == oid)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result
