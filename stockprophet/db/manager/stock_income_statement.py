import datetime

from sqlalchemy.orm.session import Session
from sqlalchemy import exc, select, update, delete, asc, desc

from stockprophet.db.model.stock import Stock
from stockprophet.db.model.stock_type import StockType
from stockprophet.db.model.stock_season_date import StockSeasonDate
from stockprophet.db.model.stock_income_statement import StockIncomeStatement
from stockprophet.utils import get_logger


logger = get_logger(__name__)


def create_api(s: Session, data_list: list = None) -> bool:
    """依據資料清單建立每季綜合損益表"""
    result = False
    if data_list is None:
        data_list = []
    try:
        if data_list:
            s.execute(StockIncomeStatement.__table__.insert(), data_list)
            s.commit()
            result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


def read_api(s: Session, code: str,
             start_date: datetime.date = None, end_date: datetime.date = None,
             order_desc: bool = False, limit: int = 100) -> list:
    """依據股票代號查詢每季綜合損益表資料"""
    result = []
    try:
        i_obj = StockIncomeStatement.__table__.c
        s_obj = Stock.__table__.c
        d_obj = StockSeasonDate.__table__.c
        stmt = select([
            i_obj.id, d_obj.date, s_obj.code, s_obj.name,
            i_obj.net_sales, i_obj.cost_of_goods_sold, i_obj.gross_profit, i_obj.operating_expenses,
            i_obj.operating_income, i_obj.total_non_op_income_expenses, i_obj.pre_tax_income,
            i_obj.income_tax_expense, i_obj.net_income, i_obj.other_comprehensive_income,
            i_obj.consolidated_net_income, i_obj.eps]
        ).select_from(
            StockIncomeStatement.__table__.join(
                StockSeasonDate.__table__, d_obj.id == i_obj.stock_date_id
            ).join(
                Stock.__table__, s_obj.id == i_obj.stock_id
            )).where(s_obj.code == code)

        if start_date is not None:
            stmt = stmt.where(d_obj.date >= start_date)
        if end_date is not None:
            stmt = stmt.where(d_obj.date <= end_date)

        if order_desc:
            stmt = stmt.order_by(desc(d_obj.date))
        else:
            stmt = stmt.order_by(asc(d_obj.date))

        if limit != 0:
            stmt = stmt.limit(limit)
        for r in s.execute(stmt):
            result.append({
                'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3],
                'net_sales': r[4], 'cost_of_goods_sold': r[5], 'gross_profit': r[6],
                'operating_expenses': r[7], 'operating_income': r[8], 'total_non_op_income_expenses': r[9],
                'pre_tax_income': r[10], 'income_tax_expense': r[11], 'net_income': r[12],
                'other_comprehensive_income': r[13], 'consolidated_net_income': r[14],
                'eps': r[15]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


def readall_api(s: Session, type_s: str, stock_date: datetime.date = None) -> list:
    """根據日期找出當季所有個股的綜合損益表資料"""
    if stock_date is None:
        stock_date = datetime.datetime.today().date()
    result = []
    try:
        s_obj = Stock.__table__.c
        i_obj = StockIncomeStatement.__table__.c
        d_obj = StockSeasonDate.__table__.c
        t_obj = StockType.__table__.c
        subquery = select([t_obj.id])\
            .where(t_obj.name == type_s).limit(1)
        stmt = select([
            i_obj.id, d_obj.date, s_obj.code, s_obj.name,
            i_obj.net_sales, i_obj.cost_of_goods_sold, i_obj.gross_profit, i_obj.operating_expenses,
            i_obj.operating_income, i_obj.total_non_op_income_expenses, i_obj.pre_tax_income,
            i_obj.income_tax_expense, i_obj.net_income, i_obj.other_comprehensive_income,
            i_obj.consolidated_net_income, i_obj.eps]
        ).select_from(
            StockIncomeStatement.__table__.join(
                StockSeasonDate.__table__, d_obj.id == i_obj.stock_date_id
            ).join(
                Stock.__table__, s_obj.id == i_obj.stock_id
            )
        ).where(
            d_obj.date == stock_date
        ).where(s_obj.stock_type_id == subquery).order_by(asc(i_obj.stock_id))

        for r in s.execute(stmt):
            result.append({
                'id': r[0], 'date': r[1], 'code': r[2], 'name': r[3],
                'net_sales': r[4], 'cost_of_goods_sold': r[5], 'gross_profit': r[6],
                'operating_expenses': r[7], 'operating_income': r[8], 'total_non_op_income_expenses': r[9],
                'pre_tax_income': r[10], 'income_tax_expense': r[11], 'net_income': r[12],
                'other_comprehensive_income': r[13], 'consolidated_net_income': r[14],
                'eps': r[15]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新綜合損益表資料"""
    result = False
    if update_data is None:
        update_data = {}
    try:
        if update_data:
            obj = StockIncomeStatement.__table__.c
            stmt = update(StockIncomeStatement.__table__)\
                .where(obj.id == oid).values(update_data)
            s.execute(stmt)
            s.commit()
            result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


def delete_api(s: Session, oid: int) -> bool:
    """依據id刪除綜合損益表資料"""
    result = False
    try:
        obj = StockIncomeStatement.__table__.c
        stmt = delete(StockIncomeStatement.__table__).where(obj.id == oid)
        s.execute(stmt)
        s.commit()
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result
