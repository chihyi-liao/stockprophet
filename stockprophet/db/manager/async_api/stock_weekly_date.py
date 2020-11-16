import datetime

from databases import Database
from sqlalchemy import exc, insert, select, update, delete, asc

from stockprophet.db.model.stock import stock_table
from stockprophet.db.model.stock_type import stock_type_table
from stockprophet.db.model.stock_weekly_date import stock_weekly_date_table
from stockprophet.db.model.stock_weekly_history import stock_weekly_history_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


async def create_api(db: Database, data_list: list = None) -> bool:
    """依據資料清單建立股市日期"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(stock_weekly_date_table).values(data_list)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def read_api(db: Database, date: datetime.date) -> list:
    """依據名稱查詢股市日期"""
    result = []
    query = select(
        [stock_weekly_date_table.c.id, stock_weekly_date_table.c.date]
    ).where(stock_weekly_date_table.c.date == date).limit(1)
    try:
        for r in await db.fetch_all(query):
            result.append({'id': r[0], 'date': r[1]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def readall_api(db: Database, type_s: str) -> list:
    """根據股市類型查詢所有股市日期(不重複)"""
    result = []

    subquery = select([stock_type_table.c.id]).where(stock_type_table.c.name == type_s).limit(1)
    query = select(
        [stock_weekly_date_table.c.id, stock_weekly_date_table.c.date]
    ).select_from(
        stock_weekly_history_table.join(
            stock_weekly_date_table, stock_weekly_date_table.id == stock_weekly_history_table.c.stock_date_id
        ).join(
            stock_table, stock_table.c.id == stock_weekly_history_table.c.stock_id
        )
    ).order_by(asc(stock_weekly_date_table.c.id)).where(stock_table.c.stock_type_id == subquery).distinct()

    try:
        for r in await db.fetch_all(query):
            result.append({'id': r[0], 'date': r[1]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def update_api(db: Database, oid: int, update_data: dict = None) -> bool:
    """依據id更新股市日期"""
    result = False
    if not update_data:
        return result

    query = update(stock_weekly_date_table).where(stock_weekly_date_table.c.id == oid).values(update_data)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def delete_api(db: Database, oid: int) -> bool:
    """依據id刪除股市日期"""
    result = False
    query = delete(stock_weekly_date_table).where(stock_weekly_date_table.c.id == oid)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result
