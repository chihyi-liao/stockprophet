from sqlalchemy.orm.session import Session
from sqlalchemy import exc, select, update, delete

from stockprophet.db.model.stock_metadata import StockMetadata
from stockprophet.utils import get_logger


logger = get_logger(__name__)


def create_api(s: Session, data_list: list = None) -> bool:
    """建立 metadata 資料表, 限制只有一筆紀錄在資料表"""
    result = False
    if data_list is None:
        data_list = []

    # 檢查是否已存在資料
    try:
        obj = StockMetadata.__table__.c
        stmt = select([obj.id]).limit(1)
        tmp = [{'id': r[0]} for r in s.execute(stmt)]
        if len(tmp) > 0:
            return result
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
        return result

    # 檢查輸入資料是否一筆
    if len(data_list) == 1:
        try:
            if data_list:
                s.execute(StockMetadata.__table__.insert(), data_list)
                s.commit()
                result = True
        except exc.SQLAlchemyError as e:
            logger.error(str(e))
        finally:
            return result
    return result


def read_api(s: Session) -> list:
    """依據名稱查詢股市型態"""
    result = []
    try:
        obj = StockMetadata.__table__.c
        stmt = select([
            obj.id, obj.tse_stock_info_update_date,
            obj.tse_weekly_history_update_date,
            obj.tse_monthly_history_update_date,
            obj.otc_stock_info_update_date,
            obj.otc_weekly_history_update_date,
            obj.otc_monthly_history_update_date,
            obj.mops_income_statement_update_date,
            obj.mops_balance_sheet_update_date,
        ]).limit(1)
        for r in s.execute(stmt):
            result.append({
                'id': r[0], 'tse_stock_info_update_date': r[1],
                'tse_weekly_history_update_date': r[2],
                'tse_monthly_history_update_date': r[3],
                'otc_stock_info_update_date': r[4],
                'otc_weekly_history_update_date': r[5],
                'otc_monthly_history_update_date': r[6],
                'mops_income_statement_update_date': r[7],
                'mops_balance_sheet_update_date': r[8]})
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


def update_api(s: Session, oid: int, update_data: dict = None) -> bool:
    """依據id更新 metadata"""
    result = False
    if update_data is None:
        update_data = {}
    try:
        if update_data:
            obj = StockMetadata.__table__.c
            stmt = update(StockMetadata.__table__)\
                .where(obj.id == oid).values(update_data)
            s.execute(stmt)
            s.commit()
            result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result


def delete_api(s: Session, oid: int) -> bool:
    """依據id刪除 metadata"""
    result = False
    try:
        obj = StockMetadata.__table__.c
        stmt = delete(StockMetadata.__table__).where(obj.id == oid)
        s.execute(stmt)
        s.commit()
        result = True
    except exc.SQLAlchemyError as e:
        logger.error(str(e))
    finally:
        return result
