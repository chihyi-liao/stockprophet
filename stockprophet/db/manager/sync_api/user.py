from sqlalchemy import exc
from sqlalchemy.orm.session import Session
from sqlalchemy import insert, select, update, delete, asc
from stockprophet.db.model.user import user_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單使用者資訊"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(user_table).values(data_list)
    try:
        s.execute(query)
        s.commit()
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
        s.rollback()
        s.close()
    finally:
        return result


def read_api(s: Session, email: str) -> list:
    """依據電子郵件查詢使用者資訊"""
    result = []
    query = select(
        [user_table.c.id, user_table.c.name, user_table.c.email, user_table.c.password]
    ).where(user_table.c.email == email).limit(1)
    try:
        for r in s.execute(query):
            result.append({'id': r[0], 'name': r[1], 'email': r[2], 'password': r[3]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
        s.rollback()
        s.close()
    finally:
        return result


def readall_api(s: Session, oid: int = 0, offset: int = 0, limit: int = 100) -> list:
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
        for r in s.execute(query):
            result.append({'id': r[0], 'name': r[1], 'email': r[2], 'password': r[3]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
        s.rollback()
        s.close()
    finally:
        return result


def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新使用者資訊"""
    result = False
    if update_data is None:
        update_data = {}

    if not update_data:
        return result

    query = update(user_table).where(user_table.c.id == oid).values(update_data)
    try:
        s.execute(query)
        s.commit()
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
        s.rollback()
        s.close()
    finally:
        return result


def delete_api(s: Session, oid: int) -> bool:
    """依據id刪除使用者資訊"""
    result = False
    stmt = delete(user_table).where(user_table.c.id == oid)
    try:
        s.execute(stmt)
        s.commit()
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
        s.rollback()
        s.close()
    finally:
        return result
