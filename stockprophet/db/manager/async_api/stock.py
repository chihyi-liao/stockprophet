from databases import Database
from sqlalchemy import exc, insert, select, update, delete, asc

from stockprophet.db.model.stock import stock_table
from stockprophet.db.model.stock_category import stock_category_table
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

    query = insert(stock_table).values(data_list)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def read_api(db: Database, type_s: str, code: str) -> list:
    """依據代號查詢股市"""
    result = []

    subquery = select([stock_type_table.c.id]).where(stock_type_table.c.name == type_s).limit(1)
    query = select(
        [stock_table.c.id, stock_table.c.code, stock_table.c.name, stock_table.c.is_alive,
         stock_type_table.c.name, stock_category_table.c.name,
         stock_table.c.stock_type_id, stock_table.c.stock_category_id]
    ).where(
        stock_table.c.code == code
    ).where(
        stock_table.c.stock_type_id == subquery
    ).limit(1)
    try:
        for r in await db.fetch_all(query):
            result.append({
                'id': r[0], 'code': r[1], 'name': r[2], 'is_alive': r[3],
                'type': r[4], 'category': r[5], 'stock_type_id': r[6], 'stock_category_id': r[7]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def readall_api(db: Database, type_s: str = '', category_s: str = '', is_alive: bool = None) -> list:
    """查詢所有股市型態"""
    result = []
    query = select([
        stock_table.c.id, stock_table.c.code, stock_table.c.name, stock_table.c.is_alive,
        stock_type_table.c.name, stock_category_table.c.name,
        stock_table.c.stock_type_id, stock_table.c.stock_category_id]
    ).select_from(
        stock_table.join(
            stock_type_table, stock_type_table.c.id == stock_table.c.stock_type_id
        ).join(
            stock_category_table, stock_category_table.c.id == stock_table.c.stock_category_id
        )
    )
    if type_s:
        query = query.where(stock_type_table.c.name == type_s)
    if category_s:
        query = query.where(stock_category_table.c.name == category_s)
    if is_alive is not None:
        query = query.where(stock_table.c.is_alive == is_alive)

    query = query.order_by(asc(stock_table.c.id))
    try:
        for r in await db.fetch_all(query):
            result.append({
                'id': r[0], 'code': r[1], 'name': r[2], 'is_alive': r[3],
                'type': r[4], 'category': r[5], 'stock_type_id': r[6], 'stock_category_id': r[7]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def update_api(db: Database, oid: int, update_data: dict = None) -> bool:
    """依據id更新股市"""
    result = False
    if update_data is None:
        update_data = {}

    if not update_data:
        return result

    query = update(stock_table).where(stock_table.c.id == oid).values(update_data)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def delete_api(db: Database, oid: int) -> bool:
    """依據id刪除股市"""
    result = False
    query = delete(stock_table).where(stock_table.c.id == oid)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result
