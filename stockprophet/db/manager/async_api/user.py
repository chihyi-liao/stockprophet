from databases import Database
from sqlalchemy import exc, insert, select, update, delete, asc

from stockprophet.db.model.user import user_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


async def create_api(db: Database, data_list: list = None) -> bool:
    """依據資料清單使用者資訊"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(user_table).values(data_list)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def read_api(db: Database, email: str) -> list:
    """依據資料清單使用者資訊"""
    result = []
    query = select(
        [user_table.c.id, user_table.c.name, user_table.c.email, user_table.c.password]
    ).where(user_table.c.email == email).limit(1)
    try:
        for r in await db.fetch_all(query):
            result.append({'id': r[0], 'name': r[1], 'email': r[2], 'password': r[3]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def readall_api(db: Database, oid: int = 0, offset: int = 0, limit: int = 100) -> list:
    """查詢所有使用者資訊"""
    result = []
    query = select(
        [user_table.c.id, user_table.c.name, user_table.c.email, user_table.c.password]
    ).where(user_table.c.id >= oid).order_by(asc(user_table.c.id))

    if offset:
        query = query.offset(offset)

    if limit:
        query = query.limit(limit)

    try:
        for r in await db.fetch_all(query):
            result.append({'id': r[0], 'name': r[1], 'email': r[2], 'password': r[3]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def update_api(db: Database, oid: int, update_data: dict = None) -> bool:
    result = False
    if not update_data:
        return result

    query = update(user_table).where(user_table.c.id == oid).values(update_data)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def delete_api(db: Database, oid: int) -> bool:
    """依據資料清單使用者資訊"""
    result = False
    query = delete(user_table).where(user_table.c.id == oid)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result
