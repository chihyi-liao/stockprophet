from databases import Database
from sqlalchemy import exc, insert, select, update, delete, asc

from stockprophet.db.model.stock import stock_table
from stockprophet.db.model.stock_type import stock_type_table
from stockprophet.db.model.stock_metadata import stock_metadata_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


async def create_api(db: Database, data_list: list = None) -> bool:
    """依據資料清單建立metadata資料"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(stock_metadata_table).values(data_list)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def read_api(db: Database, code: str) -> list:
    """依據股票代號查詢metadata"""
    result = []

    query = select([
        stock_metadata_table.c.id, stock_table.c.code, stock_table.c.name,
        stock_metadata_table.c.daily_history_create_date, stock_metadata_table.c.daily_history_update_date,
        stock_metadata_table.c.weekly_history_create_date, stock_metadata_table.c.weekly_history_update_date,
        stock_metadata_table.c.monthly_history_create_date, stock_metadata_table.c.monthly_history_update_date,
        stock_metadata_table.c.income_create_date, stock_metadata_table.c.income_update_date,
        stock_metadata_table.c.balance_create_date, stock_metadata_table.c.balance_update_date]
    ).select_from(
        stock_metadata_table.join(
            stock_table, stock_table.c.id == stock_metadata_table.c.stock_id
        )
    ).where(stock_table.c.code == code).limit(1)

    try:
        for r in await db.fetch_all(query):
            result.append({
                'id': r[0], 'code': r[1], 'name': r[2],
                'daily_history_create_date': r[3], 'daily_history_update_date': r[4],
                'weekly_history_create_date': r[5], 'weekly_history_update_date': r[6],
                'monthly_history_create_date': r[7], 'monthly_history_update_date': r[8],
                'income_create_date': r[9], 'income_update_date': r[10],
                'balance_create_date': r[11], 'balance_update_date': r[12]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def readall_api(db: Database, type_s: str) -> list:
    """查詢所有個股的metadata資料"""
    result = []

    subquery = select([stock_type_table.c.id]).where(stock_type_table.c.name == type_s).limit(1)
    query = select([
        stock_metadata_table.c.id, stock_table.c.code, stock_table.c.name,
        stock_metadata_table.c.daily_history_create_date, stock_metadata_table.c.daily_history_update_date,
        stock_metadata_table.c.weekly_history_create_date, stock_metadata_table.c.weekly_history_update_date,
        stock_metadata_table.c.monthly_history_create_date, stock_metadata_table.c.monthly_history_update_date,
        stock_metadata_table.c.income_create_date, stock_metadata_table.c.income_update_date,
        stock_metadata_table.c.balance_create_date, stock_metadata_table.c.balance_update_date]
    ).select_from(
        stock_metadata_table.join(
            stock_table, stock_table.c.id == stock_metadata_table.c.stock_id
        )
    ).where(
        stock_table.c.stock_type_id == subquery
    ).order_by(asc(stock_table.c.code))

    try:
        for r in await db.fetch_all(query):
            result.append({
                'id': r[0], 'code': r[1], 'name': r[2],
                'daily_history_create_date': r[3], 'daily_history_update_date': r[4],
                'weekly_history_create_date': r[5], 'weekly_history_update_date': r[6],
                'monthly_history_create_date': r[7], 'monthly_history_update_date': r[8],
                'income_create_date': r[9], 'income_update_date': r[10],
                'balance_create_date': r[11], 'balance_update_date': r[12]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def update_api(db: Database, oid: int, update_data: dict = None) -> bool:
    """依據id更新metadata資料"""
    result = False
    if update_data is None:
        update_data = {}

    if not update_data:
        return result

    query = update(stock_metadata_table).where(stock_metadata_table.c.id == oid).values(update_data)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def delete_api(db: Database, oid: int) -> bool:
    """依據id刪除metadata資料"""
    result = False
    query = delete(stock_metadata_table).where(stock_metadata_table.c.id == oid)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result
