from databases import Database
from sqlalchemy import exc, insert, select, update, delete, asc

from stockprophet.db.model.stock_type import stock_type_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


async def create_api(db: Database, data_list: list = None) -> bool:
    """依據資料清單建立股市型態"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(stock_type_table).values(data_list)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def read_api(db: Database, name: str) -> list:
    """依據名稱查詢股市型態"""
    result = []
    query = select(
        [stock_type_table.c.id, stock_type_table.c.name]
    ).where(stock_type_table.c.name == name).limit(1)
    try:
        for r in await db.fetch_all(query):
            result.append({'id': r[0], 'name': r[1]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def readall_api(db: Database) -> list:
    """查詢所有股市型態"""
    result = []
    query = select([stock_type_table.c.id, stock_type_table.c.name]).order_by(asc(stock_type_table.c.id))
    try:
        for r in await db.fetch_all(query):
            result.append({'id': r[0], 'name': r[1]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def update_api(db: Database, oid: int, update_data: dict = None) -> bool:
    """依據id更新股市型態"""
    result = False
    if not update_data:
        return result

    query = update(stock_type_table).where(stock_type_table.c.id == oid).values(update_data)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def delete_api(db: Database, oid: int) -> bool:
    """依據id刪除股市型態"""
    result = False
    query = delete(stock_type_table).where(stock_type_table.c.id == oid)
    try:
        await db.execute(query)
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result
