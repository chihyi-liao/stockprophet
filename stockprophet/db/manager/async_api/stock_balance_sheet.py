import datetime

from databases import Database
from sqlalchemy import exc, insert, select, update, delete, asc, desc

from stockprophet.db.model.stock import stock_table
from stockprophet.db.model.stock_type import stock_type_table
from stockprophet.db.model.stock_season_date import stock_season_date_table
from stockprophet.db.model.stock_balance_sheet import stock_balance_sheet_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


async def create_api(db: Database, data_list: list = None) -> bool:
    """依據資料清單建立每季資產負債表"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(stock_balance_sheet_table).values(data_list)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def read_api(db: Database, code: str, start_date: datetime.date = None, end_date: datetime.date = None,
                   order_desc: bool = False, limit: int = 100) -> list:
    """依據股票代號查詢每季資產負債表資料"""
    result = []

    query = select([
        stock_balance_sheet_table.c.id, stock_season_date_table.c.date,
        stock_table.c.code, stock_table.c.name,
        stock_balance_sheet_table.c.intangible_assets, stock_balance_sheet_table.c.total_assets,
        stock_balance_sheet_table.c.total_liabs, stock_balance_sheet_table.c.short_term_borrowing,
        stock_balance_sheet_table.c.total_current_assets, stock_balance_sheet_table.c.total_non_current_assets,
        stock_balance_sheet_table.c.total_current_liabs, stock_balance_sheet_table.c.total_non_current_liabs,
        stock_balance_sheet_table.c.accrued_payable, stock_balance_sheet_table.c.other_payable,
        stock_balance_sheet_table.c.capital_reserve, stock_balance_sheet_table.c.common_stocks,
        stock_balance_sheet_table.c.total_stocks, stock_balance_sheet_table.c.inventories,
        stock_balance_sheet_table.c.prepaid, stock_balance_sheet_table.c.shareholders_net_income]
    ).select_from(
        stock_balance_sheet_table.join(
            stock_season_date_table, stock_season_date_table.c.id == stock_balance_sheet_table.c.stock_date_id
        ).join(
            stock_table, stock_table.c.id == stock_balance_sheet_table.c.stock_id
        )
    ).where(stock_table.c.code == code)

    if start_date is not None:
        query = query.where(stock_season_date_table.c.date >= start_date)
    if end_date is not None:
        query = query.where(stock_season_date_table.c.date <= end_date)

    if order_desc:
        query = query.order_by(desc(stock_season_date_table.c.date))
    else:
        query = query.order_by(asc(stock_season_date_table.c.date))

    if limit != 0:
        query = query.limit(limit)

    try:
        for r in await db.fetch_all(query):
            result.append({
                'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3],
                'intangible_assets': r[4], 'total_assets': r[5], 'total_liabs': r[6],
                'short_term_borrowing': r[7], 'total_current_assets': r[8], 'total_non_current_assets': r[9],
                'total_current_liabs': r[10], 'total_non_current_liabs': r[11], 'accrued_payable': r[12],
                'other_payable': r[13], 'capital_reserve': r[14], 'common_stocks': r[15],
                'total_stocks': r[16], 'inventories': r[17], 'prepaid': r[18], 'shareholders_net_income': r[19]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def readall_api(db: Database, type_s: str, stock_date: datetime.date = None) -> list:
    """根據日期找出當季所有個股的資產負債表資料"""
    result = []
    if stock_date is None:
        stock_date = datetime.datetime.today().date()

    subquery = select([stock_type_table.c.id]).where(stock_type_table.c.name == type_s).limit(1)
    query = select([
        stock_balance_sheet_table.c.id, stock_season_date_table.c.date,
        stock_table.c.code, stock_table.c.name,
        stock_balance_sheet_table.c.intangible_assets, stock_balance_sheet_table.c.total_assets,
        stock_balance_sheet_table.c.total_liabs, stock_balance_sheet_table.c.short_term_borrowing,
        stock_balance_sheet_table.c.total_current_assets, stock_balance_sheet_table.c.total_non_current_assets,
        stock_balance_sheet_table.c.total_current_liabs, stock_balance_sheet_table.c.total_non_current_liabs,
        stock_balance_sheet_table.c.accrued_payable, stock_balance_sheet_table.c.other_payable,
        stock_balance_sheet_table.c.capital_reserve, stock_balance_sheet_table.c.common_stocks,
        stock_balance_sheet_table.c.total_stocks, stock_balance_sheet_table.c.inventories,
        stock_balance_sheet_table.c.prepaid, stock_balance_sheet_table.c.shareholders_net_income]
    ).select_from(
        stock_balance_sheet_table.join(
            stock_season_date_table, stock_season_date_table.c.id == stock_balance_sheet_table.c.stock_date_id
        ).join(
            stock_table, stock_table.c.id == stock_balance_sheet_table.c.stock_id
        )
    ).where(
        stock_season_date_table.c.date == stock_date
    ).where(
        stock_table.c.stock_type_id == subquery
    ).order_by(asc(stock_balance_sheet_table.c.stock_id))

    try:
        for r in await db.fetch_all(query):
            result.append({
                'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3],
                'intangible_assets': r[4], 'total_assets': r[5], 'total_liabs': r[6],
                'short_term_borrowing': r[7], 'total_current_assets': r[8], 'total_non_current_assets': r[9],
                'total_current_liabs': r[10], 'total_non_current_liabs': r[11], 'accrued_payable': r[12],
                'other_payable': r[13], 'capital_reserve': r[14], 'common_stocks': r[15],
                'total_stocks': r[16], 'inventories': r[17], 'prepaid': r[18], 'shareholders_net_income': r[19]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def update_api(db: Database, oid: int, update_data: dict = None) -> bool:
    """依據id更新資產負債表資料"""
    result = False
    if not update_data:
        return result

    query = update(stock_balance_sheet_table).where(stock_balance_sheet_table.c.id == oid).values(update_data)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


async def delete_api(db: Database, oid: int) -> bool:
    """依據id刪除資產負債表資料"""
    result = False
    query = delete(stock_balance_sheet_table).where(stock_balance_sheet_table.c.id == oid)
    try:
        await db.execute(query)
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result
