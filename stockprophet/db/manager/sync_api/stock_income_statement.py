import datetime

from sqlalchemy.orm.session import Session
from sqlalchemy import exc, insert, select, update, delete, asc, desc

from stockprophet.db.model.stock import stock_table
from stockprophet.db.model.stock_type import stock_type_table
from stockprophet.db.model.stock_season_date import stock_season_date_table
from stockprophet.db.model.stock_income_statement import stock_income_statement_table
from stockprophet.db.log import get_logger


logger = get_logger(__name__)


def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立每季綜合損益表"""
    result = False
    if data_list is None:
        data_list = []

    if len(data_list) == 0:
        return result

    query = insert(stock_income_statement_table).values(data_list)
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
    """依據股票代號查詢每季綜合損益表資料"""
    result = []

    query = select([
        stock_income_statement_table.c.id, stock_season_date_table.c.date,
        stock_table.c.code, stock_table.c.name,
        stock_income_statement_table.c.net_sales, stock_income_statement_table.c.cost_of_goods_sold,
        stock_income_statement_table.c.gross_profit, stock_income_statement_table.c.operating_expenses,
        stock_income_statement_table.c.operating_income, stock_income_statement_table.c.total_non_op_income_expenses,
        stock_income_statement_table.c.pre_tax_income, stock_income_statement_table.c.income_tax_expense,
        stock_income_statement_table.c.net_income, stock_income_statement_table.c.other_comprehensive_income,
        stock_income_statement_table.c.consolidated_net_income, stock_income_statement_table.c.eps]
    ).select_from(
        stock_income_statement_table.join(
            stock_season_date_table, stock_season_date_table.c.id == stock_income_statement_table.c.stock_date_id
        ).join(
            stock_table, stock_table.c.id == stock_income_statement_table.c.stock_id
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
        for r in s.execute(query):
            result.append({
                'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3],
                'net_sales': r[4], 'cost_of_goods_sold': r[5], 'gross_profit': r[6],
                'operating_expenses': r[7], 'operating_income': r[8], 'total_non_op_income_expenses': r[9],
                'pre_tax_income': r[10], 'income_tax_expense': r[11], 'net_income': r[12],
                'other_comprehensive_income': r[13], 'consolidated_net_income': r[14],
                'eps': r[15]})
    except exc.SQLAlchemyError as e:
        s.rollback()
        s.close()
        logger.error(str(e))
    finally:
        return result


def readall_api(s: Session, type_s: str, stock_date: datetime.date = None) -> list:
    """根據日期找出當季所有個股的綜合損益表資料"""
    result = []
    if stock_date is None:
        stock_date = datetime.datetime.today().date()

    subquery = select([stock_type_table.c.id]).where(stock_type_table.c.name == type_s).limit(1)
    query = select([
        stock_income_statement_table.c.id, stock_season_date_table.c.date,
        stock_table.c.code, stock_table.c.name,
        stock_income_statement_table.c.net_sales, stock_income_statement_table.c.cost_of_goods_sold,
        stock_income_statement_table.c.gross_profit, stock_income_statement_table.c.operating_expenses,
        stock_income_statement_table.c.operating_income, stock_income_statement_table.c.total_non_op_income_expenses,
        stock_income_statement_table.c.pre_tax_income, stock_income_statement_table.c.income_tax_expense,
        stock_income_statement_table.c.net_income, stock_income_statement_table.c.other_comprehensive_income,
        stock_income_statement_table.c.consolidated_net_income, stock_income_statement_table.c.eps]
    ).select_from(
        stock_income_statement_table.join(
            stock_season_date_table, stock_season_date_table.c.id == stock_income_statement_table.c.stock_date_id
        ).join(
            stock_table, stock_table.c.id == stock_income_statement_table.c.stock_id
        )
    ).where(
        stock_season_date_table.c.date == stock_date
    ).where(
        stock_table.c.stock_type_id == subquery
    ).order_by(asc(stock_income_statement_table.c.stock_id))

    try:
        for r in s.execute(query):
            result.append({
                'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3],
                'net_sales': r[4], 'cost_of_goods_sold': r[5], 'gross_profit': r[6],
                'operating_expenses': r[7], 'operating_income': r[8], 'total_non_op_income_expenses': r[9],
                'pre_tax_income': r[10], 'income_tax_expense': r[11], 'net_income': r[12],
                'other_comprehensive_income': r[13], 'consolidated_net_income': r[14],
                'eps': r[15]})
    except exc.SQLAlchemyError as e:
        s.rollback()
        s.close()
        logger.error(str(e))
    finally:
        return result


def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新綜合損益表資料"""
    result = False
    if not update_data:
        return result

    query = update(stock_income_statement_table).where(stock_income_statement_table.c.id == oid).values(update_data)
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
    """依據id刪除綜合損益表資料"""
    result = False
    query = delete(stock_income_statement_table).where(stock_income_statement_table.c.id == oid)
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
