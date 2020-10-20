import calendar
import datetime
from typing import List


def date_range(start_date: datetime.date, end_date: datetime.date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + datetime.timedelta(n)


def week_range(date: datetime.date):
    start = date - datetime.timedelta(days=date.weekday())
    end = start + datetime.timedelta(days=6)
    return start, end


def week_of_year(date: datetime.date):
    year, week, _ = date.isocalendar()
    return year, week


def month_range(date: datetime.date):
    start_date = date.replace(day=1)
    end_date = start_date.replace(day=calendar.monthrange(date.year, date.month)[1])
    return start_date, end_date


def season_range(date: datetime.date):
    seasons = [4, 4, 4, 1, 1, 2, 2, 2, 3, 3, 3, 4]
    season = seasons[date.month - 1]
    year = date.year
    if season == 1:
        start_date = datetime.date(year, 4, 1)
        end_date = datetime.date(year, 6, 1) - datetime.timedelta(days=1)
    elif season == 2:
        start_date = datetime.date(year, 6, 1)
        end_date = datetime.date(year, 9, 1) - datetime.timedelta(days=1)
    elif season == 3:
        start_date = datetime.date(year, 9, 1)
        end_date = datetime.date(year, 12, 1) - datetime.timedelta(days=1)
    else:
        month = date.month
        if month in [1, 2, 3]:
            start_date = datetime.date(year-1, 12, 1)
            end_date = datetime.date(year, 4, 1) - datetime.timedelta(days=1)
        else:
            start_date = datetime.date(year, 12, 1)
            end_date = datetime.date(year+1, 4, 1) - datetime.timedelta(days=1)
    return start_date, end_date


def latest_year_season(date: datetime.date):
    year = date.year
    month = date.month
    if month in [12, 1, 2, 3]:
        if month != 12:
            year = year - 1
        return year, 3
    elif month in [4, 5]:
        return year-1, 4
    elif month in [6, 7, 8]:
        return year, 1
    elif month in [9, 10, 11]:
        return year, 2


def get_latest_season_date(date: datetime.date):
    year, season = latest_year_season(date)
    if season == 1:
        start_date = datetime.date(year, 4, 1)
    elif season == 2:
        start_date = datetime.date(year, 6, 1)
    elif season == 3:
        start_date = datetime.date(year, 9, 1)
    else:
        start_date = datetime.date(year, 12, 1)
    return start_date


def get_offset_date_by_days(date: datetime.date, n: int):
    return date - datetime.timedelta(days=n)


def date_to_year_season(date: datetime.date):
    seasons = [4, 4, 4, 1, 1, 2, 2, 2, 3, 3, 3, 4]
    season = seasons[date.month-1]
    month = date.month
    if month in [1, 2, 3]:
        year = date.year - 1
    else:
        year = date.year
    return year, season


def is_weekday(date: datetime.date):
    if date.weekday() in [5, 6]:
        return True
    return False


def get_latest_stock_date(holiday_dates: List[str] = None):
    """取得最後一個交易日, 取下午四點後今天才算最後一天"""
    collect_dates = []
    if holiday_dates is None:
        holiday_dates = []

    for str_date in holiday_dates:
        collect_dates.append(datetime.datetime.strptime(str_date, "%Y-%m-%d").date())

    now = datetime.datetime.now()
    crawl_time = datetime.datetime(now.year, now.month, now.day, 16, 0, 0, 0)
    end_date = now.date()
    if now < crawl_time:
        end_date = end_date - datetime.timedelta(days=1)
    else:
        end_date = now.date()

    while True:
        if end_date in collect_dates or is_weekday(end_date):
            end_date = end_date - datetime.timedelta(days=1)
            continue
        return end_date


def check_all_holiday(start_date: datetime.date, end_date: datetime.date,
                      holiday_dates: list, trade_dates: list):
    """判斷某個範圍時間內是否都是放假日"""
    date_list = [dt for dt in date_range(start_date, end_date)]
    total = len(date_list)
    for i, dt in enumerate(date_list, start=1):
        # 節慶休市日
        if dt in holiday_dates:
            total = total - 1
            continue
        # 週末而且不是補上班日期
        if is_weekday(dt) and dt not in trade_dates:
            total = total - 1
            continue
    if total == 0:
        return True
    return False


def check_crawler_date_settings(start_date: datetime.date, end_date: datetime.date, default: datetime.date):
    if start_date < default:
        err_msg = "'start_date'小於預設, 預設為: %s" % (start_date.strftime("%Y-%m-%d"), )
        raise Exception(err_msg)

    if end_date < start_date:
        err_msg = "'end_date'小於'start_date'"
        raise Exception(err_msg)

    if end_date > datetime.datetime.today().date():
        err_msg = "'end_date'不可大於今天"
        raise Exception(err_msg)
