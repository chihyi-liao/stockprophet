from datetime import date, datetime, timedelta

from stockprophet.cli.common import progressbar, date_to_integer
from stockprophet.cli.ta.core import compute
from stockprophet.cli.ta.core.kdj import check_kdj_buy_point, check_kdj_sell_point
from stockprophet.cli.sim.core import account
from stockprophet.crawler.utils.date import date_range, week_range, month_range, is_weekday
from stockprophet.crawler.utils.common import get_stock_dates, get_stock_revive_data
from stockprophet.db import get_session
from stockprophet.db.manager import sync_api as db_mgr


def do_kdj(code: str, principal: int, init_vol: int, start_date: date, end_date: date, n_day: int,
           use_weekly: bool, use_monthly: bool, scalar: int, roi_limit: float, progress: bool = False) -> list:
    result = []

    # 取得所有節慶的休市日期(包含颱風假)
    holiday_list = []
    date_data = get_stock_dates()
    for str_date in date_data.get("market_holiday", []):
        holiday_list.append(datetime.strptime(str_date, "%Y-%m-%d").date())

    # 取得補上班日的股市日期
    trading_list = []
    for str_date in date_data.get("additional_trading_day", []):
        trading_list.append(datetime.strptime(str_date, "%Y-%m-%d").date())

    # 檢查輸入日期
    if start_date >= end_date:
        return result

    # 檢查指定的資料表
    if use_weekly and use_monthly:
        return result

    # 設定使用的資料表
    if use_weekly:
        table = db_mgr.stock_weekly_history
    elif use_monthly:
        table = db_mgr.stock_monthly_history
    else:
        table = db_mgr.stock_daily_history

    # 設定初始變數
    my_account = account.Account(principal=principal)
    last_price = 0.0
    last_date = None
    tmp_vol = init_vol
    s = get_session()

    # 依據起始跟結束日期開始迭代日期
    total = date_to_integer(end_date) - date_to_integer(start_date)
    for current_date in date_range(start_date=start_date, end_date=end_date):
        # 處理進度條
        if progress:
            cur = date_to_integer(current_date) - date_to_integer(start_date)
            progressbar(cur=cur, total=total)

        # 檢查現在日期是否為國定假日
        if current_date in holiday_list:
            continue

        # 檢查日期是否為週末而且不是補上班日
        if is_weekday(current_date) and current_date not in trading_list:
            continue

        # 因為KDJ需要過去一段時間的歷史資料,因此需設定資料表的起始日期
        if use_weekly:
            week_date, _ = week_range(current_date)
            if current_date != week_date:
                continue
            stock_start_date = week_date - timedelta(days=n_day*scalar*7)
        elif use_monthly:
            month_date, _ = month_range(current_date)
            if current_date != month_date:
                continue
            stock_start_date = month_date - timedelta(days=n_day*scalar*30)
        else:
            stock_start_date = current_date - timedelta(days=n_day*scalar)

        # 從資料庫取得歷史資料
        stock_data = table.read_api(
            s, code=code, start_date=stock_start_date, end_date=current_date, limit=0)

        # 收集每日最高價, 最低價, 收盤價
        high_values = []
        low_values = []
        close_values = []
        for val in stock_data:
            if not val['co']:
                continue
            high_values.append(val['hi'])
            low_values.append(val['lo'])
            close_values.append(val['co'])

        # 檢查是否有收盤價
        if len(close_values) == 0:
            continue

        # 取得當日股價並更新初始變數
        price = close_values[-1]
        last_price = price
        last_date = current_date

        # 根據最高價, 最低價與收盤價取得的KDJ計算資料
        k_data, d_data, j_data = compute.kdj(
            high_values=high_values, low_values=low_values, close_values=close_values, n=n_day)

        # 買點判斷
        if check_kdj_buy_point(k_data=k_data, d_data=d_data, j_data=j_data) is True:
            # 計算該股價夠買的量
            max_vol = my_account.get_max_buy_volume(price=price)
            if my_account.total_volume == 0:  # 若沒有股票則以 init_vol 當基底買量
                vol = init_vol if max_vol >= init_vol else max_vol
                my_account.buy(current_date, price=price, volume=vol)
            else:  # 若已有股票則以 init_vol 的倍數買量
                vol = tmp_vol * 2
                if max_vol >= vol:
                    tmp_vol = vol
                    my_account.buy(current_date, price=price, volume=vol)

        # 賣點判斷
        if check_kdj_sell_point(k_data=k_data, d_data=d_data, j_data=j_data) is True:
            total_volume = my_account.get_total_volume()
            avg_price = my_account.get_avg_price()
            if total_volume:  # 確認是否有總量
                if price >= avg_price:  # 獲利情況
                    tmp_vol = init_vol
                    sell_vol = total_volume
                else:  # 虧損情況
                    sell_vol = 0
                    roi = my_account.get_roi(price)
                    if roi <= roi_limit:  # 根據roi每次認賠一半
                        sell_vol = int(total_volume / 2)

                if sell_vol > 0:
                    my_account.sell(current_date, price=price, volume=sell_vol)

    # 若有總量則強制賣出, 來取得最後一天的成果
    total_volume = my_account.get_total_volume()
    if total_volume > 0 and last_date:
        my_account.sell(last_date, last_price, total_volume)

    result = my_account.get_records()
    return result


def do_all_kdj(principal: int, init_vol: int, start_date: date, end_date: date, n_day: int,
               use_weekly: bool, use_monthly: bool, scalar: int, top_size: int,
               limit_price: float, roi_limit: float, progress: bool = False) -> list:
    result = []

    # 取得減資股市資料
    revive_data = {}
    for data in get_stock_revive_data():
        code = data['code']
        if code not in revive_data:
            revive_data[code] = list()
        revive_data[code].append(data)

    s = get_session()
    # 取得所有的股市資料
    stock_list = db_mgr.stock.readall_api(s, is_alive=True)
    total = len(stock_list)
    for no, stock in enumerate(stock_list):
        # 處理進度條
        if progress:
            progressbar(cur=no+1, total=total)

        code = stock['code']
        name = stock['name']
        # 排除個股查詢時間內有減資情況
        flag = False
        if code in revive_data:
            for d in revive_data[code]:
                if start_date < datetime.strptime(d['revive_date'], "%Y-%m-%d").date() < end_date:
                    flag = True
                    continue
        if flag:
            continue

        # 從最新的歷史資料限制股價
        history_list = db_mgr.stock_daily_history.read_api(s, code, end_date=end_date, order_desc=True, limit=1)
        if len(history_list) == 0:
            continue
        else:
            price = history_list[0]['co']
            if not price or price > limit_price:
                continue

        # 取得 kdj 的執行成果
        kdj_data_list = do_kdj(code=code, principal=principal, init_vol=init_vol,
                               start_date=start_date, end_date=end_date, n_day=n_day,
                               use_weekly=use_weekly, use_monthly=use_monthly, scalar=scalar,
                               roi_limit=roi_limit, progress=False)

        # 檢查kdj資料是否存在
        if len(kdj_data_list) == 0:
            continue

        # 取得總資產
        total_assets = kdj_data_list[-1][9]

        # 若總資產比top list最小的值大, 則排序之後取切片
        if total_assets > principal:
            stock_name = "%s(%s)" % (name, code)
            if len(result) < top_size:
                diff = round((total_assets-principal) * 100 / principal, 2)
                result.append([None, stock_name, price, principal, total_assets, diff])
                result.sort(key=lambda r: r[4])
            else:
                for item in result:
                    if total_assets > item[4]:
                        diff = round((total_assets - principal) * 100 / principal, 2)
                        data = [None, stock_name, price, principal, total_assets, diff]
                        result.append(data)
                        result.sort(key=lambda r: r[4])
                        result = result[-top_size:]
                    break

    # 打上排序編號
    for i, item in enumerate(result):
        item[0] = i+1

    return result
