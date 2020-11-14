import random
import threading
import time
from datetime import datetime, date

from stockprophet.db import create_local_session, db_lock
from stockprophet.db import manager as db_mgr
from stockprophet.utils import get_logger
from .utils.common import (
    HttpRequest, get_stock_dates, convert_to_float, convert_to_int,
    convert_to_direction, convert_change_to_string
)
from .utils.date import get_latest_stock_date, date_range, is_weekday, check_crawler_date_settings
from .utils.compute import (
    calc_weekly_history_table, calc_monthly_history_table, calc_metadata
)

OTC_CATEGORY = [
    ('02', '食品工業'), ('03', '塑膠工業'), ('04', '紡織纖維'), ('05', '電機機械'),
    ('06', '電器電纜'), ('08', '玻璃陶瓷'), ('10', '鋼鐵工業'), ('11', '橡膠工業'),
    ('14', '建材營造'), ('15', '航運業'), ('16', '觀光事業'), ('17', '金融保險'),
    ('18', '貿易百貨'), ('20', '其他'), ('21', '化學工業'), ('22', '生技醫療業'),
    ('23', '油電燃氣業'), ('24', '半導體業'), ('25', '電腦及週邊設備業'), ('26', '光電業'),
    ('27', '通信網路業'), ('28', '電子零組件業'), ('29', '電子通路業'), ('30', '資訊服務業'),
    ('31', '其他電子業'), ('32', '文化創意業'), ('33', '農業科技業'), ('34', '電子商務業')
]
STOCK_URL = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
STOCK_REVIVE_URL = "https://www.tpex.org.tw/web/stock/exright/revivt/revivt_result.php"

logger = get_logger(__name__)


def fetch_stock_revive_info(retry: int = 10) -> list:
    """
    歷年上櫃減資資訊資料表
    輸出格式: [{'code': '4153', 'name': '鈺緯', 'revive_date': '2020-10-19', 'old_price': '27.20', 'new_price': '30.62'}]
    """
    result = []
    start_date = date(2013, 1, 1)
    end_date = datetime.today()
    req = HttpRequest()
    kwargs = dict()
    kwargs['headers'] = req.default_headers()
    kwargs['params'] = {
        'o': 'json', 'l': 'zh',
        'd': '{}/{:02d}/{:02d}'.format(start_date.year - 1911, start_date.month, start_date.day),
        'ed': '{}/{:02d}/{:02d}'.format(end_date.year - 1911, end_date.month, end_date.day),
        '_': int(time.time() * 1000)}

    for i in range(retry):
        req.wait_interval = random.randint(3, 5)
        resp = req.send_data(method='GET', url=STOCK_REVIVE_URL, **kwargs)
        if resp.status_code == 200:
            data = resp.json()
            if not data:
                continue

            stocks = data.get('aaData', [])
            for stock in stocks:
                code = stock[1]
                # 只抓取代碼長度為4的資料
                if len(code) != 4:
                    continue

                str_zh_date = str(stock[0])
                if len(str_zh_date) != 7:
                    continue
                year = 1911 + int(str_zh_date[0:3])
                month = int(str_zh_date[3:5])
                day = int(str_zh_date[5:7])
                revive_date = date(year, month, day)
                name = stock[2]
                old_price = stock[3]
                new_price = stock[4]
                result.append({
                    'code': code, 'name': name, 'revive_date': revive_date.strftime("%Y-%m-%d"),
                    'old_price': old_price, 'new_price': new_price})
            break
        else:
            logger.warning("無法取得所有上櫃減資歷史資資料")

    return result


def fetch_stock_history(dt: date, retry: int = 10) -> list:
    """
    依據日期的抓取該日所有股市交易行情
    輸出格式: [('1240', '茂生農經', '48,000', '', '2,609,300', '54.60', '55.30', '54.00', '55.30', '+', '0.5'), (...)]
    """
    req = HttpRequest()
    result = []
    kwargs = dict()
    kwargs['headers'] = req.default_headers()
    kwargs['params'] = {
        'o': 'json', 'l': 'zh',
        'd': '{}/{:02d}/{:02d}'.format(dt.year - 1911, dt.month, dt.day),
        'se': 'EW'}

    for i in range(retry):
        req.wait_interval = random.randint(3, 5)
        resp = req.send_data(method='GET', url=STOCK_URL, **kwargs)
        if resp.status_code == 200:
            data = resp.json()
            if not data:
                continue

            stocks = data.get('aaData', [])
            for stock in stocks:
                code = stock[0]
                # 只抓取代碼長度為4的資料
                if len(code) != 4:
                    continue
                name = stock[1]
                opening_price = stock[4]
                highest_price = stock[5]
                lowest_price = stock[6]
                closing_price = stock[2]
                trade_volume = stock[7]
                trade_value = stock[8]
                direction, change = convert_change_to_string(stock[3].replace(' ', ''))
                result.append(tuple([code, name, trade_volume, '', trade_value,
                                     opening_price, highest_price, lowest_price, closing_price,
                                     direction, change]))
            break
        else:
            logger.warning("無法取得'%s'上櫃每日收盤行情資料", dt.strftime("%Y-%m-%d"))

    return result


def fetch_stock_category(dt: date, retry: int = 10) -> dict:
    """
    依據日期抓取所有類股資料
    輸出格式: {'食品工業': [('1258', '其祥-KY'), ('1264', '德麥'), ('1796', '金穎生技'),
                         ('4205', '中華食'), ('4207', '環泰'), ('4712', '南璋')]
    """
    req = HttpRequest()
    result = dict()
    kwargs = dict()
    kwargs['headers'] = req.default_headers()
    kwargs['params'] = {
        'o': 'json', 'l': 'zh',
        'd': '{}/{:02d}/{:02d}'.format(dt.year - 1911, dt.month, dt.day)}

    for i, (_type, _category) in enumerate(OTC_CATEGORY, start=1):
        kwargs['params']['se'] = _type
        for n in range(retry):
            req.wait_interval = random.randint(3, 5)
            resp = req.send_data(method='GET', url=STOCK_URL, **kwargs)
            if resp.status_code == 200:
                data = resp.json()
                if not data:
                    continue

                stocks = data.get('aaData', [])
                _data = []
                for stock in stocks:
                    code = stock[0]
                    # 只抓取代碼長度為4的資料
                    if len(code) != 4:
                        continue
                    name = stock[1]
                    _data.append(tuple([code, name]))
                result[_category] = _data
                logger.info("取得 '%s'(%s) 資料", dt.strftime("%Y-%m-%d"), _category)
                break
            else:
                logger.warning("無法取得%s上櫃類股(%s)資料", dt.strftime("%Y-%m-%d"), _category)
                if n == retry - 1:
                    result = {}
                    return result
    return result


class CrawlerTask(threading.Thread):
    def __init__(self, start_date: date = None, end_date: date = None, build_period_table=False):
        threading.Thread.__init__(self)
        self._stock_type = "otc"
        self._session = create_local_session()
        self._loss_fetch = []
        self._build_period_table = build_period_table
        if not start_date:
            self.start_date = self.default_date()
        else:
            self.start_date = start_date

        if not end_date:
            self.end_date = self.latest_date()
        else:
            self.end_date = end_date

        # 檢查日期設定
        check_crawler_date_settings(self.start_date, self.end_date, self.default_date())

    @staticmethod
    def default_date() -> date:
        return date(2007, 7, 2)

    @staticmethod
    def latest_date() -> date:
        date_data = get_stock_dates()
        return get_latest_stock_date(date_data.get("market_holiday", []))

    def build_stock_table(self) -> bool:
        logger.info("build otc stock table")

        type_list = db_mgr.stock_type.read_api(self._session, name=self._stock_type)
        if len(type_list) == 0:
            logger.error("無法從資料庫取得<stock_type>資料")
            return False

        dt = self.latest_date()
        fetch_data = fetch_stock_category(dt)
        if not fetch_data:
            logger.error("無法取得 '%s' 完整類股的資料" % (self._stock_type, ))
            return False

        type_dict = type_list[0]
        # 從資料庫取所有仍上櫃的股票資料
        db_stocks = db_mgr.stock.readall_api(
            self._session, type_s=self._stock_type, is_alive=True)

        alive_stocks_curr = {}  # 目前仍上櫃股
        alive_stocks_db = {}    # 在資料庫內的上櫃股
        for r in db_stocks:
            alive_stocks_db[r['code']] = r['name']

        for c_name, values in fetch_data.items():
            category_list = db_mgr.stock_category.read_api(self._session, name=c_name)
            if len(category_list) == 0:
                logger.warning("無法從資料庫取得<stock_category '%s'>資料", c_name)
                continue

            category_dict = category_list[0]
            for code, name in values:
                db_lock.acquire()
                stock_list = db_mgr.stock.read_api(self._session, type_s=self._stock_type, code=code)
                alive_stocks_curr[code] = name

                # 若從資料庫找不到, 則建立該股資料 len(stock_list)
                if len(stock_list) == 0:
                    logger.info("建立上櫃資料: 名稱'%s'(代號:%s)", name, code)
                    insert_data = [{'code': code, 'name': name, 'is_alive': True,
                                    'stock_type_id': type_dict['id'], 'stock_category_id': category_dict['id']}]
                    db_mgr.stock.create_api(self._session, data_list=insert_data)
                    db_lock.release()
                    continue
                else:
                    db_lock.release()

                stock_id = stock_list[0]['id']
                # 存在資料庫但已下櫃, 之後又恢復上櫃
                if code not in alive_stocks_db:
                    logger.warning("股市名稱'%s'(代號:%s) 恢復上櫃", name, code)
                    update_data = {'is_alive': True}
                    db_mgr.stock.update_api(self._session, oid=stock_id, update_data=update_data)

        # 若資料庫的上櫃股, 已不在證交所網站上則設為下櫃
        for code, name in alive_stocks_db.items():
            if code not in alive_stocks_curr.keys():
                logger.warning("股市名稱'%s'(代號:%s) 已下櫃或暫停交易", name, code)
                update_data = {'is_alive': False}
                stock_list = db_mgr.stock.read_api(self._session, type_s=self._stock_type, code=code)
                db_mgr.stock.update_api(self._session, oid=stock_list[0]['id'], update_data=update_data)

        return True

    def build_stock_daily_history_table(self) -> bool:
        logger.info("build otc stock_daily_history table")

        date_data = get_stock_dates()
        # 取得所有節慶的休市日期(包含颱風假)
        holiday_list = []
        for str_date in date_data.get("market_holiday", []):
            holiday_list.append(datetime.strptime(str_date, "%Y-%m-%d").date())

        # 取得補上班日的股市日期
        trading_list = []
        for str_date in date_data.get("additional_trading_day", []):
            trading_list.append(datetime.strptime(str_date, "%Y-%m-%d").date())

        # 取得每日歷史行情的日期範圍
        daily_history_dates = []
        history_date_list = db_mgr.stock_daily_date.readall_api(self._session, self._stock_type)
        for r in history_date_list:
            daily_history_dates.append(r['date'])

        # 取得上櫃股類型
        type_list = db_mgr.stock_type.read_api(self._session, name=self._stock_type)
        if len(type_list) == 0:
            logger.error("無法從資料庫取得'<stock_type>'資料")
            return False

        stocks = {}
        stock_type_id = type_list[0]['id']

        # 取得未分類股市類別, 目的是已下市或暫停交易的歷史資料
        unknown_category_id = None
        unknown_category = db_mgr.stock_category.read_api(self._session, name='未分類')
        if unknown_category:
            unknown_category_id = unknown_category[0]['id']

        # 從資料庫取所有仍上櫃的股票資料
        db_stocks = db_mgr.stock.readall_api(
            self._session, type_s=self._stock_type, is_alive=True)
        for r in db_stocks:
            stocks[r['code']] = r['id']

        # 根據指定的日期範圍開始抓取資料
        for current_date in date_range(self.start_date, self.end_date):
            # 當抓取日期是已存在的歷史資料或是節慶休市日則跳過
            if current_date in daily_history_dates or current_date in holiday_list:
                continue

            # 當抓取日期是週末而且不是補上班日期則跳過
            if is_weekday(current_date) and current_date not in trading_list:
                continue

            # 從證交所取得當日的所有股市資料
            stock_data = fetch_stock_history(current_date)
            if not stock_data:
                self._loss_fetch.append(current_date)
                logger.warning("無法取得'%s'的每日收盤歷史資料", current_date.strftime("%Y-%m-%d"))
                continue

            # 從資料庫找尋是否有stock_date, 沒有則建立
            db_lock.acquire()
            if len(db_mgr.stock_daily_date.read_api(self._session, date=current_date)) == 0:
                if not db_mgr.stock_daily_date.create_api(self._session, data_list=[{'date': current_date}]):
                    logger.error("無法建立'%s'日期資料表", current_date.strftime("%Y-%m-%d"))
                    db_lock.release()
                    continue
                else:
                    db_lock.release()
            else:
                db_lock.release()

            # 取得 stock_date_id
            stock_date_list = db_mgr.stock_daily_date.read_api(self._session, date=current_date)
            if len(stock_date_list) == 0:
                logger.error("無法取得'%s'日期資料表", current_date.strftime("%Y-%m-%d"))
                continue

            data_list = []
            stock_date_id = stock_date_list[0]['id']
            for item in stock_data:
                code = item[0]
                name = item[1]
                stock_id = stocks.get(code)
                direction = convert_to_direction(item[9])
                op = convert_to_float(item[5])
                hi = convert_to_float(item[6])
                lo = convert_to_float(item[7])
                co = convert_to_float(item[8])
                vol = convert_to_int(item[2])
                val = convert_to_int(item[4])
                ch = direction * convert_to_float(item[10])
                if not stock_id:  # 已下櫃或暫停交易
                    if unknown_category_id:
                        db_lock.acquire()
                        unknown_stock_list = db_mgr.stock.read_api(
                            self._session, type_s=self._stock_type, code=code)
                        if len(unknown_stock_list) == 0:  # 不存在stock table
                            insert_data = [{'code': code, 'name': name, 'is_alive': False,
                                            'stock_type_id': stock_type_id, 'stock_category_id': unknown_category_id}]
                            db_mgr.stock.create_api(self._session, data_list=insert_data)
                            db_lock.release()
                            unknown_stock_list = db_mgr.stock.read_api(
                                self._session, type_s=self._stock_type, code=code)
                        else:
                            db_lock.release()

                        if len(unknown_stock_list) == 0:
                            logger.warning("無法取得'未分類'的類股")
                            continue
                        stock_id = unknown_stock_list[0]['id']
                data_list.append(
                    {'op': op, 'hi': hi, 'lo': lo, 'co': co, 'vol': vol, 'val': val,
                     'ch': ch, 'stock_id': stock_id, 'stock_date_id': stock_date_id})

            # 建立每日收盤行情
            if data_list:
                logger.info("建立%s收盤行情", current_date.strftime("%Y-%m-%d"))
                db_mgr.stock_daily_history.create_api(self._session, data_list=data_list)

        return True

    def build_weekly_history_table(self) -> bool:
        if calc_weekly_history_table(self._session, stock_type=self._stock_type,
                                     start_date=self.start_date, end_date=self.end_date):
            return True
        return False

    def build_monthly_history_table(self) -> bool:
        if calc_monthly_history_table(self._session, stock_type=self._stock_type,
                                      start_date=self.start_date, end_date=self.end_date):
            return True
        return False

    def update_metadata_table(self) -> bool:
        if calc_metadata(self._session, type_s=self._stock_type,
                         start_date=self.default_date(), end_date=self.latest_date()):
            return True
        return False

    def run(self):
        logger.info("Starting OTC thread")
        metadata_list = []
        stock_list = db_mgr.stock.readall_api(self._session, type_s=self._stock_type, is_alive=True)
        if len(stock_list) > 0:
            stock = stock_list[0]
            metadata_list = db_mgr.stock_metadata.read_api(self._session, code=stock['code'])

        if self._build_period_table:
            if len(metadata_list) == 1:
                weekly_history_update_date = metadata_list[0]['weekly_history_update_date']
                if weekly_history_update_date:
                    self.start_date = weekly_history_update_date
            self.build_weekly_history_table()

            if len(metadata_list) == 1:
                monthly_history_update_date = metadata_list[0]['monthly_history_update_date']
                if monthly_history_update_date:
                    self.start_date = monthly_history_update_date
            self.build_monthly_history_table()

            self.update_metadata_table()
            self._session.close()
            logger.info("Finish OTC thread")
            return
        else:
            if self.build_stock_table() is True:
                self.build_stock_daily_history_table()
                self.update_metadata_table()

            if len(self._loss_fetch) != 0:
                logger.warning("缺少 %s 的日期資料, 請重新執行爬蟲", self._loss_fetch)

            self._session.close()
            logger.info("Finish OTC thread")
            return
