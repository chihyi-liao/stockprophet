from databases import Database
from sqlalchemy import exc, insert, select, update, delete, asc

from stockprophet.db.model.stock import stock_table
from stockprophet.db.model.stock_type import stock_type_table
from stockprophet.db.model.stock_capital_reduction import stock_capital_reduction_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


async def create_api(db: Database, data_list: list = None) -> bool:
    """依據資料清單建立減資股市資料"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(stock_capital_reduction_table).values(data_list)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def read_api(db: Database, code: str) -> list:
    """依據股票代號查詢減資股市資訊"""
    result = []

    query = select([
        stock_capital_reduction_table.c.id, stock_table.c.code, stock_table.c.name,
        stock_capital_reduction_table.c.old_price, stock_capital_reduction_table.c.new_price,
        stock_capital_reduction_table.c.reason, stock_capital_reduction_table.c.new_kilo_stock,
        stock_capital_reduction_table.c.give_back_per_stock, stock_capital_reduction_table.c.stop_tran_date,
        stock_capital_reduction_table.c.revive_date]
    ).select_from(
        stock_capital_reduction_table.join(
            stock_table, stock_table.c.id == stock_capital_reduction_table.c.stock_id
        )
    ).where(stock_table.c.code == code)

    try:
        for r in await db.fetch_all(query):
            result.append({
                'id': r[0], 'code': r[1], 'name': r[2],
                'old_price': r[3], 'new_price': r[4],
                'reason': r[5], 'new_kilo_stock': r[6],
                'give_back_per_stock': r[7], 'stop_tran_date': r[8],
                'revive_date': r[9]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def readall_api(db: Database, type_s: str) -> list:
    """查詢所有個股的減資股市資料"""
    result = []

    subquery = select([stock_type_table.c.id]).where(stock_type_table.c.name == type_s).limit(1)
    query = select([
        stock_capital_reduction_table.c.id, stock_table.c.code, stock_table.c.name,
        stock_capital_reduction_table.c.old_price, stock_capital_reduction_table.c.new_price,
        stock_capital_reduction_table.c.reason, stock_capital_reduction_table.c.new_kilo_stock,
        stock_capital_reduction_table.c.give_back_per_stock, stock_capital_reduction_table.c.stop_tran_date,
        stock_capital_reduction_table.c.revive_date]
    ).select_from(
        stock_capital_reduction_table.join(
            stock_table, stock_table.c.id == stock_capital_reduction_table.c.stock_id
        )
    ).where(
        stock_table.c.stock_type_id == subquery
    ).order_by(asc(stock_capital_reduction_table.c.revive_date))

    try:
        for r in await db.execute(query):
            result.append({
                'id': r[0], 'code': r[1], 'name': r[2],
                'old_price': r[3], 'new_price': r[4],
                'reason': r[5], 'new_kilo_stock': r[6],
                'give_back_per_stock': r[7], 'stop_tran_date': r[8],
                'revive_date': r[9]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def update_api(db: Database, oid: int, update_data: dict = None) -> bool:
    """依據id更新減資股市資料"""
    result = False
    if update_data is None:
        update_data = {}

    if not update_data:
        return result

    query = update(stock_capital_reduction_table).where(stock_capital_reduction_table.c.id == oid).values(update_data)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def delete_api(db: Database, oid: int) -> bool:
    """依據id刪除減資股市資料"""
    result = False
    query = delete(stock_capital_reduction_table).where(stock_capital_reduction_table.c.id == oid)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result
