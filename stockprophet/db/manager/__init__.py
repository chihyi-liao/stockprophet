from functools import wraps
from sqlalchemy import exc

from stockprophet.utils import get_logger

logger = get_logger(__name__)


def db_api(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except exc.SQLAlchemyError as e:
            logger.error(str(e))
            s = args[0]
            s.rollback()
            s.close()
            raise
    return wrapper


try:
    from . import (
        stock, stock_metadata, stock_type, stock_category,
        stock_daily_date, stock_daily_history,
        stock_weekly_date, stock_weekly_history,
        stock_monthly_date, stock_monthly_history,
        stock_income_statement, stock_balance_sheet,
        stock_season_date, stock_monthly_revenue
    )
except ImportError:
    raise

