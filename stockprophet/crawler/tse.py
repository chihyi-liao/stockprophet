import random
import threading
import time
from datetime import datetime, date

from lxml import html

from stockprophet.db import get_local_session, db_lock
from stockprophet.db.manager import sync_api as db_mgr
from stockprophet.utils import get_logger
from .utils.common import (
    HttpRequest, get_stock_dates, convert_to_float, convert_to_int, convert_to_direction
)
from .utils.date import get_latest_stock_date, date_range, is_weekday, check_crawler_date_settings
from .utils.compute import (
    calc_weekly_history_table, calc_monthly_history_table, calc_metadata
)

TSE_CATEGORY = [
    ('01', '水泥工業'), ('02', '食品工業'), ('03', '塑膠工業'), ('04', '紡織纖維'),
    ('05', '電機機械'), ('06', '電器電纜'), ('08', '玻璃陶瓷'), ('09', '造紙工業'),
    ('10', '鋼鐵工業'), ('11', '橡膠工業'), ('12', '汽車工業'), ('14', '建材營造'),
    ('15', '航運業'), ('16', '觀光事業'), ('17', '金融保險'), ('18', '貿易百貨'),
    ('20', '其他'), ('21', '化學工業'), ('22', '生技醫療業'), ('23', '油電燃氣業'),
    ('24', '半導體業'), ('25', '電腦及週邊設備業'), ('26', '光電業'), ('27', '通信網路業'),
    ('28', '電子零組件業'), ('29', '電子通路業'), ('30', '資訊服務業'), ('31', '其他電子業'),
    ('0099P', 'ETF')
]
STOCK_URL = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
STOCK_REVIVE_URL = "https://www.twse.com.tw/exchangeReport/TWTAUU"
STOCK_REVIVE_DETAIL_URL = "https://www.twse.com.tw/zh/exchange/TWTAVUDetail"


logger = get_logger(__name__)


def fetch_stock_revive_info(start_date: date = None, end_date: date = None, retry: int = 10) -> list:
    """
    歷年上櫃減資資訊資料表
    輸出格式: [ {'code': '2321', 'name': '東訊', 'revive_date': '2020-10-19', 'old_price': '3.79', 'new_price': '12.63'}]
    """
    result = []
    if not start_date:
        start_date = date(2011, 1, 1)

    if not end_date:
        end_date = datetime.today()

    req = HttpRequest()
    kwargs = dict()
    kwargs['headers'] = req.default_headers()
    kwargs['params'] = {'response': 'json',
                        'lang': 'zh',
                        'strDate': start_date.strftime('%Y%m%d'),
                        'endDate': end_date.strftime('%Y%m%d'),
                        '_': int(time.time() * 1000)}
    for i in range(retry):
        req.wait_interval = random.randint(5, 10)
        resp = req.send_data(method='GET', url=STOCK_REVIVE_URL, **kwargs)
        if resp.status_code == 200:
            try:
                data = resp.json()
                if not data:
                    continue
            except Exception as e:
                logger.warning(str(e))
                continue

            # 證交所有時會出現錯誤的查詢日期資訊： {"stat": "查詢日期小於93年2月11日，請重新查詢!"}
            if data.get('stat') and "請重新查詢" in data['stat']:
                continue

            # 處理資料字串
            for key, value in data.items():
                _fields = 'fields'
                _key = key.lower()
                if _fields in _key and len(value) == 11:
                    postfix = _key.replace(_fields, '')
                    data_list = data.get('data' + postfix, [])
                    for r in data_list:
                        code = r[1]
                        # 只抓取代碼長度為4的資料
                        if len(code) != 4:
                            continue

                        zh_date_list = r[0].split('/')
                        if len(zh_date_list) != 3:
                            continue
                        year = 1911 + int(zh_date_list[0])
                        month = int(zh_date_list[1])
                        day = int(zh_date_list[2])
                        revive_date = date(year, month, day)
                        name = r[2]
                        old_price = round(float(r[3]), 2)
                        new_price = round(float(r[4]), 2)
                        reason = r[9]
                        data = {'code': code, 'name': name,
                                'revive_date': revive_date,
                                'old_price': old_price, 'new_price': new_price, 'reason': reason}

                        param = r[10].split('  ')[0]
                        detail_param = param.split('?')[-1]
                        param_list = detail_param.split('&')
                        stk_no = ''
                        file_date = ''
                        for p in param_list:
                            if 'FILE_DATE' in p:
                                file_date = p.split('=')[-1]
                            if 'STK_NO' in p:
                                stk_no = p.split('=')[-1]

                        if stk_no and file_date:
                            detail_data = fetch_stock_revive_detail_info(stk_no, file_date)
                            for k, v in detail_data.items():
                                data[k] = v

                        logger.info("取得減資資料: %s" % (data, ))
                        result.append(data)
                    break
            break
        else:
            logger.warning("無法取得所有上市減資歷史資資料")
    return result


def fetch_stock_revive_detail_info(stk_no: str, file_date: str, retry: int = 10) -> dict:
    result = {}
    req = HttpRequest()
    kwargs = dict()
    kwargs['headers'] = req.default_headers()
    kwargs['params'] = {'STK_NO': stk_no, 'FILE_DATE': file_date}

    translate_table = {
        '每壹仟股換發新股票': 'new_kilo_stock',
        '停止買賣日期': 'stop_tran_date',
        '每股退還股款': 'give_back_per_stock'}

    for i in range(retry):
        req.wait_interval = random.randint(5, 10)
        resp = req.send_data(method='GET', url=STOCK_REVIVE_DETAIL_URL, **kwargs)
        if resp.status_code == 200:
            tree = html.fromstring(resp.text)
            root = "/html/body/div[1]/div[1]/div/div/main/article/table/tr"
            for dom in tree.xpath(root):
                text = ''
                for j, td in enumerate(dom.xpath("td[position()>=1 and position()<=2]/text()")):
                    if j == 0:
                        text = ''
                        for key, value in translate_table.items():
                            if key in td.strip():
                                text = value
                                break
                    else:
                        if text:
                            value = td.split(' ')[0]
                            if text in ["new_kilo_stock", "give_back_per_stock"]:
                                try:
                                    result[text] = round(float(value), 2)
                                except ValueError:
                                    logger.error("%s 轉型錯誤" % (text, ))
                            else:
                                date_list = value.split('/')
                                if len(date_list) == 3:
                                    year = int(date_list[0]) + 1911
                                    stop_trans_date = date(int(year), int(date_list[1]), int(date_list[2]))
                                    result[text] = stop_trans_date
            break
        else:
            logger.warning("無法取得%s上市減資明細" % (stk_no, ))
    return result


def fetch_stock_history(dt: date, retry: int = 10) -> list:
    """
    依據日期的抓取該日所有股市交易行情
    輸出格式: [('0050', '元大台灣50', '6,274,033', '3,797', '637,408,220', '101.75', '102.35', '101.15', '102.30',
              '<p style= color:green>-</p>', '0.70', '102.25', '31', '102.30', '8', '0.00'), (...)]
    """
    req = HttpRequest()
    result = []
    kwargs = dict()
    kwargs['headers'] = req.default_headers()
    kwargs['params'] = {'response': 'json',
                        'lang': 'zh',
                        'date': dt.strftime('%Y%m%d'),
                        'type': 'ALLBUT0999'}
    for i in range(retry):
        req.wait_interval = random.randint(5, 10)
        resp = req.send_data(method='GET', url=STOCK_URL, **kwargs)
        if resp.status_code == 200:
            try:
                data = resp.json()
                if not data:
                    continue
            except Exception as e:
                logger.warning(str(e))
                continue

            # 證交所有時會出現錯誤的查詢日期資訊： {"stat": "查詢日期小於93年2月11日，請重新查詢!"}
            if data.get('stat') and "請重新查詢" in data['stat']:
                continue

            # 處理資料字串
            for key, value in data.items():
                _fields = 'fields'
                _key = key.lower()
                if _fields in _key and len(value) == 16:
                    _data = []
                    postfix = _key.replace(_fields, '')
                    data_list = data.get('data' + postfix, [])
                    for r in data_list:
                        # 只抓取代碼長度為4的資料
                        if len(r[0]) != 4:
                            continue
                        _data.append(tuple(r))
                    result.extend(_data)
                    break
            break
        else:
            logger.warning("無法取得'%s'上市每日收盤行情資料", dt.strftime("%Y-%m-%d"))
    return result


def fetch_stock_category(dt: date, retry: int = 10) -> dict:
    """
    依據日期抓取所有類股資料
    輸出格式: {'水泥工業': [('1101', '台泥'), ('1102', '亞泥'), ('1103', '嘉泥'), ('1104', '環泥'),
                         ('1108', '幸福'), ('1109', '信大'), ('1110', '東泥')]}
    """
    req = HttpRequest()
    result = dict()
    kwargs = dict()
    kwargs['headers'] = req.default_headers()
    kwargs['params'] = {'response': 'json',
                        'lang': 'zh',
                        'date': dt.strftime('%Y%m%d')}

    for i, (_type, _category) in enumerate(TSE_CATEGORY, start=1):
        kwargs['params']['type'] = _type
        for n in range(retry):
            req.wait_interval = random.randint(10, 15)
            resp = req.send_data(method='GET', url=STOCK_URL, **kwargs)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    if not data:
                        continue
                except Exception as e:
                    logger.warning(str(e))
                    continue

                # 證交所有時會出現錯誤的查詢日期資訊： {"stat": "查詢日期小於93年2月11日，請重新查詢!"}
                if data.get('stat') and "請重新查詢" in data['stat']:
                    continue

                # 處理資料字串
                for key, value in data.items():
                    _fields = 'fields'
                    _key = key.lower()
                    if _fields in _key and len(value) >= 16:
                        _data = []
                        postfix = _key.replace(_fields, '')
                        data_list = data.get('data' + postfix, [])
                        for r in data_list:
                            code = r[0]
                            name = r[1]
                            # 只抓取代碼長度為4的資料
                            if len(code) != 4:
                                continue
                            _data.append(tuple([code, name]))
                        result[_category] = _data
                        break
                logger.info("取得 '%s'(%s) 資料", dt.strftime("%Y-%m-%d"), _category)
                break
            else:
                logger.warning("無法取得'%s'上市類股(%s)資料", dt.strftime("%Y-%m-%d"), _category)
                if n == retry - 1:  # 需確保 category 資料完整, 因此 retry 多次失敗後停止執行
                    result = {}
                    return result

    return result


class CrawlerTask(threading.Thread):
    def __init__(self, start_date: date = None, end_date: date = None, build_period_table=False):
        threading.Thread.__init__(self)
        self._stock_type = "tse"
        self._session = get_local_session()
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
        return date(2004, 2, 11)

    @staticmethod
    def latest_date() -> date:
        date_data = get_stock_dates()
        return get_latest_stock_date(date_data.get("market_holiday", []))

    def build_stock_table(self) -> bool:
        logger.info("build tse stock table")
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
        # 從資料庫取所有仍上市的股票資料
        db_stocks = db_mgr.stock.readall_api(
            self._session, type_s=self._stock_type, is_alive=True)

        alive_stocks_curr = {}  # 目前仍上市股
        alive_stocks_db = {}    # 在資料庫內的上市股
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
                stock_list = db_mgr.stock.read_api(
                    self._session, type_s=self._stock_type, code=code)
                alive_stocks_curr[code] = name

                # 若從資料庫找不到, 則建立該股資料
                if len(stock_list) == 0:
                    logger.info("建立上市資料: 名稱'%s'(代號:%s)", name, code)
                    insert_data = [{'code': code, 'name': name, 'is_alive': True,
                                    'stock_type_id': type_dict['id'], 'stock_category_id': category_dict['id']}]
                    db_mgr.stock.create_api(self._session, data_list=insert_data)
                    db_lock.release()
                    continue
                else:
                    db_lock.release()

                stock_id = stock_list[0]['id']
                # 存在資料庫但已下市, 之後又恢復上市
                if code not in alive_stocks_db:
                    logger.warning("股市名稱'%s'(代號:%s) 恢復上市", name, code)
                    update_data = {'is_alive': True}
                    db_mgr.stock.update_api(self._session, oid=stock_id, update_data=update_data)

        # 若資料庫的上市股, 已不在證交所網站上則設為下市
        for code, name in alive_stocks_db.items():
            if code not in alive_stocks_curr.keys():
                logger.warning("股市名稱'%s'(代號:%s) 已下市或暫停交易", name, code)
                update_data = {'is_alive': False}
                stock_list = db_mgr.stock.read_api(self._session, type_s=self._stock_type, code=code)
                db_mgr.stock.update_api(self._session, oid=stock_list[0]['id'], update_data=update_data)

        return True

    def build_stock_daily_history_table(self) -> bool:
        logger.info("build tse stock_daily_history table")

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

        # 取得上市股類型
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

        # 從資料庫取所有仍上市的股票資料
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

            # 建立 stock_date
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
                if not stock_id:  # 已下市或暫停交易
                    if unknown_category_id:
                        db_lock.acquire()
                        unknown_stock_list = db_mgr.stock.read_api(self._session, type_s=self._stock_type, code=code)
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

    def build_stock_capital_reduction_table(self) -> bool:
        logger.info("build tse stock_capital_reduction table")

        # 從資料庫取減資股市資料
        reduction_stocks = db_mgr.stock_capital_reduction.readall_api(self._session, type_s=self._stock_type)

        collect = {}
        # 取最後減資日期
        last_revive_date = None
        if len(reduction_stocks) > 0:
            last_revive_date = reduction_stocks[-1].get('revive_date')
            for stock in reduction_stocks:
                code = stock['code']
                if not collect.get(code):
                    collect[code] = list()
                collect[code].append(stock['revive_date'])

        data_list = fetch_stock_revive_info(start_date=last_revive_date)
        for r in data_list:
            code = r['code']
            stock_list = db_mgr.stock.read_api(self._session, type_s=self._stock_type, code=code)
            if len(stock_list) == 0:
                continue

            stock_id = stock_list[0]['id']
            data = {
                'stock_id': stock_id, 'old_price': r['old_price'], 'new_price': r['new_price'],
                'reason': r['reason'], 'new_kilo_stock': r['new_kilo_stock'],
                'give_back_per_stock': r['give_back_per_stock'], 'stop_tran_date': r['stop_tran_date'],
                'revive_date': r['revive_date']}

            if not collect.get(code):
                db_mgr.stock_capital_reduction.create_api(self._session, data_list=[data])
            else:
                code_revive_date = collect[code][-1]
                if r['revive_date'] > code_revive_date:
                    db_mgr.stock_capital_reduction.create_api(self._session, data_list=[data])

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
        logger.info("Starting TSE thread")
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
            logger.info("Finish TSE thread")
            return
        else:
            if self.build_stock_table() is True:
                self.build_stock_daily_history_table()
                self.build_stock_capital_reduction_table()
                self.update_metadata_table()

            if len(self._loss_fetch) != 0:
                logger.warning("缺少 %s 的日期資料, 請重新執行爬蟲", self._loss_fetch)

            self._session.close()
            logger.info("Finish TSE thread")
            return
