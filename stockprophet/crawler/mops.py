import random
import threading
from datetime import date, timedelta
from typing import Optional

from sqlalchemy.orm.session import Session
from lxml import html

from stockprophet.db import create_local_session, db_lock
from stockprophet.db import manager as db_mgr
from stockprophet.utils import get_logger
from .utils.date import (
    get_latest_stock_date, season_range, latest_year_season, date_to_year_season,
    date_range, check_crawler_date_settings, month_range
)
from .utils.common import HttpRequest, get_stock_dates


MOPS_URL = "https://mops.twse.com.tw/mops/web"

logger = get_logger(__name__)


def translate_income_statement(data):
    result = {}
    translate_table = {
        '營業收入合計': 'net_sales', '營業成本合計': 'cost_of_goods_sold',
        '營業毛利（毛損）淨額': 'gross_profit', '營業費用合計': 'operating_expenses',
        '營業利益（損失）': 'operating_income', '營業外收入及支出合計': 'total_non_op_income_expenses',
        '稅前淨利（淨損）': 'pre_tax_income', '所得稅費用（利益）合計': 'income_tax_expense',
        '本期淨利（淨損）': 'net_income', '其他綜合損益（淨額）': 'other_comprehensive_income',
        '本期綜合損益總額': 'consolidated_net_income', '基本每股盈餘': 'eps'}
    for key, val in data.items():
        # noinspection PyBroadException
        try:
            if key == '基本每股盈餘':
                val = float(val.replace(',', ''))
            else:
                val = int(val.replace(',', ''))
        except Exception:
            continue

        if key in translate_table.keys():
            translate_name = translate_table[key]
            result[translate_name] = val
    return result


def translate_balance_sheet(data):
    result = {}
    translate_table = {
        '無形資產': 'intangible_assets', '資產總額': 'total_assets',
        '資產總計': 'total_assets', '負債總額': 'total_liabs',
        '負債總計': 'total_liabs', '短期借款': 'short_term_borrowing',
        '流動資產合計': 'total_current_assets', '非流動資產合計': 'total_non_current_assets',
        '流動負債合計': 'total_current_liabs', '非流動負債合計': 'total_non_current_liabs',
        '應付帳款': 'accrued_payable', '其他應付款': 'other_payable',
        '資本公積': 'capital_reserve', '資本公積合計': 'capital_reserve',
        '普通股股本': 'common_stocks', '股本合計': 'total_stocks',
        '存貨': 'inventories', '預付款項': 'prepaid',
        '歸屬於母公司業主之權益': 'shareholders_net_income',
        '歸屬於母公司業主之權益合計': 'shareholders_net_income'}
    for key, val in data.items():
        # noinspection PyBroadException
        try:
            val = int(val.replace(',', ''))
        except Exception:
            continue

        if key in translate_table.keys():
            translate_name = translate_table[key]
            result[translate_name] = val
    return result


def convert_stock_type(type_s: str) -> str:
    result = {'tse': 'sii', 'otc': 'otc'}
    return result.get(type_s, '')


def check_over_run(tree: Optional[any]) -> bool:
    """檢查是否查詢過於頻繁"""
    is_overrun = False
    overrun_tree = tree.xpath("//table/tr/td/center/text()")
    for item in overrun_tree:
        if 'overrun' in item.lower() or '查詢過於頻繁' in item:
            is_overrun = True
        break
    if is_overrun:
        logger.info("爬蟲降速: 查詢過載")
        return True
    else:
        return False


def fetch_monthly_revenue(type_s: str, year: int, month: int, retry: int = 3):
    """
    取得月營收資料可以從2013/01到現在
    """
    req = HttpRequest()
    result = {}

    stock_type = convert_stock_type(type_s)
    if not stock_type:
        return result

    url = 'https://mops.twse.com.tw/nas/t21/{stock_type}/t21sc03_{year}_{month}_0.html'.format(
        stock_type=stock_type, year=year - 1911, month=month)
    kwargs = dict()
    kwargs['headers'] = req.default_headers()

    req.wait_interval = random.randint(5, 10)
    for i in range(retry):
        resp = req.send_data('GET', url, **kwargs)
        if resp.status_code == 200:
            resp.encoding = 'big5'
            tree = html.fromstring(resp.text)
            root = "/html/body/center/center/table//tr[position() >=2 and position() <= last()-1]"
            for dom in tree.xpath(root):
                stock_revenue = []
                for td in dom.xpath("td[position()>=0 and position()<=3]/text()"):
                    stock_revenue.append(td.strip())

                if len(stock_revenue) == 3:
                    code = stock_revenue[0]
                    revenue = stock_revenue[2]
                    result[code] = revenue

            if result:
                logger.warning("取得 %s %s-%s 月營收報表資料" % (type_s, year, month))
                break
        else:
            req.wait_interval = random.randint(30, 40)
            logger.warning("無法取得 %s %s-%s 月營收報表資料" % (type_s, year, month))

    return result


def fetch_income_statement(type_s: str, code: str, year: int, season: int, step: str = '1', retry: int = 3):
    """Income statement is started from 2013/01 to now, which is based on IFRSs policy
    """
    req = HttpRequest()
    url = "{base_url}/ajax_t164sb04".format(base_url=MOPS_URL)
    kwargs = dict()
    kwargs['headers'] = req.form_headers()
    kwargs['data'] = {'encodeURIComponent': '1',
                      'step': step,
                      'firstin': '1',
                      'co_id': code,
                      'year': year - 1911,
                      'season': "%02d" % season}
    result = {}
    stock_type = convert_stock_type(type_s)
    if not stock_type:
        return result

    req.wait_interval = random.randint(5, 10)
    kwargs['data']['TYPEK'] = stock_type
    for i in range(retry):
        resp = req.send_data('POST', url, **kwargs)
        if resp.status_code == 200:
            tree = html.fromstring(resp.text)
            if check_over_run(tree):
                req.wait_interval = random.randint(30, 40)
                logger.warning("股市代號: %s, 無法取得%s-%Qs綜合損益表資料(過載)", code, year, season)
                continue
            else:
                # 解析綜合損益表
                req.wait_interval = random.randint(5, 10)
                subtree = tree.xpath("//table[@class='hasBorder']/tr")
                for item in subtree:
                    titles = item.xpath(
                        "td[@style='text-align:left;white-space:nowrap;']/text()")
                    titles = ["".join(r.split()) for r in titles]
                    raw_data = item.xpath("td[@style='text-align:right;']/text()")
                    values = ["".join(r.split()) for r in raw_data]
                    if len(titles) == 1 and len(values) > 0:
                        result[titles[0]] = values[0]
                if not result:
                    logger.warning("200: %s", resp.text)
                break
        else:
            logger.warning(resp.content)
            logger.warning("股市代號: %s, 無法取得%s-Q%s綜合損益表資料", code, year, season)
    return translate_income_statement(result)


def fetch_balance_sheet(type_s: str, code: str, year: int, season: int, step: str = '1', retry: int = 3):
    """Balance sheet is started from 2013/01 to now, which is based on IFRSs policy
    """
    req = HttpRequest()
    url = "{base_url}/ajax_t164sb03".format(base_url=MOPS_URL)
    kwargs = dict()
    kwargs['headers'] = req.form_headers()
    kwargs['data'] = {'encodeURIComponent': '1',
                      'step': step,
                      'firstin': '1',
                      'co_id': code,
                      'year': year - 1911,
                      'season': "%02d" % season}
    result = {}
    stock_type = convert_stock_type(type_s)
    if not stock_type:
        return result

    req.wait_interval = random.randint(5, 10)
    kwargs['data']['TYPEK'] = stock_type
    for i in range(retry):
        resp = req.send_data('POST', url, **kwargs)
        if resp.status_code == 200:
            tree = html.fromstring(resp.text)
            if check_over_run(tree):
                req.wait_interval = random.randint(30, 40)
                logger.warning("股市代號: %s, 無法取得%s-Q%s資產負債表資料(過載)", code, year, season)
                continue
            else:
                req.wait_interval = random.randint(5, 10)
                subtree = tree.xpath("//table[@class='hasBorder']/tr")
                for item in subtree:
                    titles = item.xpath(
                        "td[@style='text-align:left;white-space:nowrap;']/text()")
                    titles = ["".join(r.split()) for r in titles]
                    raw_data = item.xpath(
                        "td[@style='text-align:right;white-space:nowrap;']/text()")
                    values = ["".join(r.split()) for r in raw_data]
                    if len(titles) == 1 and len(values) > 0:
                        result[titles[0]] = values[0]
                if not result:
                    logger.warning("200: %s", resp.text)
                break
        else:
            logger.warning(resp.content)
            logger.warning("股市代號: %s, 無法取得%s-Q%s資產負債表資料", code, year, season)
    return translate_balance_sheet(result)


def patch_balance_sheet_table(s: Session, type_s: str, code: str, start_date: date, end_date: date):
    logger.info("patch stock balance table")
    default_date = date(2013, 1, 1)

    # 排除 ETF 相關個股
    etf_list = db_mgr.stock_category.read_api(s, name='ETF')
    etf_id = None
    if len(etf_list) == 1:
        etf_id = etf_list[0]['id']
    # 金融股查找需要特定參數
    bank_list = db_mgr.stock_category.read_api(s, name='金融保險')
    bank_id = None
    if len(bank_list) == 1:
        bank_id = bank_list[0]['id']
    l_year, l_season = latest_year_season(end_date)
    stock_list = db_mgr.stock.read_api(s, type_s=type_s, code=code)
    if len(stock_list) != 1:
        return
    stock = stock_list[0]
    # 過濾 ETF 有關的股票
    if etf_id is not None and stock['stock_category_id'] == etf_id:
        return

    metadata_list = db_mgr.stock_metadata.read_api(s, code=code)
    if len(metadata_list) != 1:  # should never run here
        raise Exception("metadata list is 0")
    metadata_id = metadata_list[0]['id']
    history_create_date = metadata_list[0]['daily_history_create_date']
    if history_create_date > start_date:
        start_date = history_create_date

    if start_date <= default_date:
        _, et = season_range(default_date)
        start_date, _ = season_range(et + timedelta(days=1))
    else:
        start_date, _ = season_range(start_date)

    create_date = None
    for current_date in date_range(start_date, end_date):
        # 判斷時間已做到最新的報表
        year, season = date_to_year_season(current_date)
        if year >= l_year and season > l_season:
            break
        # 只處理每季第一天, 避免重複計算
        sn_start_date, sn_end_date = season_range(current_date)
        season_date = sn_start_date
        if current_date != season_date:
            continue

        metadata = dict()

        # 取得 season date
        db_lock.acquire()
        sn_date_list = db_mgr.stock_season_date.read_api(s, sn_start_date)
        if len(sn_date_list) == 0:
            logger.info("建立 '%s' 的season_date", sn_start_date.strftime("%Y-%m-%d"))
            db_mgr.stock_season_date.create_api(s, data_list=[{'date': sn_start_date}])
            db_lock.release()
            sn_date_list = db_mgr.stock_season_date.read_api(s, date=sn_start_date)
        else:
            db_lock.release()

        # 取得 stock_date_id
        stock_date_id = sn_date_list[0]['id']

        # 金融股要特殊參數
        if bank_id is not None and stock['stock_category_id'] == bank_id:
            data = fetch_balance_sheet(type_s, code, year, season, '2')
        elif code in ['2841']:
            data = fetch_balance_sheet(type_s, code, year, season, '2')
        else:
            data = fetch_balance_sheet(type_s, code, year, season)
        if data:
            data['stock_id'] = stock['id']
            data['stock_date_id'] = stock_date_id
            # 若該股已存在則跳過
            db_lock.acquire()
            balance_list = db_mgr.stock_balance_sheet.read_api(
                s, code=stock['code'], start_date=sn_start_date, end_date=sn_start_date, limit=1)
            if len(balance_list) > 0:
                db_mgr.stock_balance_sheet.update_api(s, oid=balance_list[0]['id'], update_data=data)
            else:
                db_mgr.stock_balance_sheet.create_api(s, data_list=[data])
            db_lock.release()
            if not create_date:
                create_date = current_date
                metadata['balance_create_date'] = create_date
            metadata['balance_update_date'] = sn_start_date
            db_mgr.stock_metadata.update_api(s, oid=metadata_id, update_data=metadata)
            logger.info(
                "%s(%s)建立 '%s' 的資產負債表" % (
                    stock['name'], stock['code'], sn_start_date.strftime("%Y-%m-%d")))
        else:
            logger.warning(
                "%s(%s)找不到'%s-%s'資產負債資料" % (stock['name'], stock['code'], year, season))


def patch_income_statement_table(s: Session, type_s: str, code: str, start_date: date, end_date: date):
    logger.info("patch stock income table")
    default_date = date(2013, 1, 1)

    # 排除 ETF 相關個股
    etf_list = db_mgr.stock_category.read_api(s, name='ETF')
    etf_id = None
    if len(etf_list) == 1:
        etf_id = etf_list[0]['id']
    # 金融股查找需要特定參數
    bank_list = db_mgr.stock_category.read_api(s, name='金融保險')
    bank_id = None
    if len(bank_list) == 1:
        bank_id = bank_list[0]['id']
    l_year, l_season = latest_year_season(end_date)
    stock_list = db_mgr.stock.read_api(s, type_s=type_s, code=code)
    if len(stock_list) != 1:
        return
    stock = stock_list[0]
    # 過濾 ETF 有關的股票
    if etf_id is not None and stock['stock_category_id'] == etf_id:
        return

    metadata_list = db_mgr.stock_metadata.read_api(s, code=code)
    if len(metadata_list) != 1:  # should never run here
        raise Exception("metadata list is 0")
    metadata_id = metadata_list[0]['id']
    history_create_date = metadata_list[0]['daily_history_create_date']
    if history_create_date and history_create_date > start_date:
        start_date = history_create_date

    if start_date <= default_date:
        _, et = season_range(default_date)
        start_date, _ = season_range(et + timedelta(days=1))
    else:
        start_date, _ = season_range(start_date)

    create_date = None
    for current_date in date_range(start_date, end_date):
        # 判斷時間已做到最新的報表
        year, season = date_to_year_season(current_date)
        if year >= l_year and season > l_season:
            break
        # 只處理每季第一天, 避免重複計算
        sn_start_date, sn_end_date = season_range(current_date)
        season_date = sn_start_date
        if current_date != season_date:
            continue

        metadata = dict()

        # 取得 season date
        db_lock.acquire()
        sn_date_list = db_mgr.stock_season_date.read_api(s, sn_start_date)
        if len(sn_date_list) == 0:
            logger.info("建立 '%s' 的season_date", sn_start_date.strftime("%Y-%m-%d"))
            db_mgr.stock_season_date.create_api(s, data_list=[{'date': sn_start_date}])
            db_lock.release()
            sn_date_list = db_mgr.stock_season_date.read_api(s, date=sn_start_date)
        else:
            db_lock.release()

        # 取得 stock_date_id
        stock_date_id = sn_date_list[0]['id']

        # 金融股要特殊參數
        if bank_id is not None and stock['stock_category_id'] == bank_id:
            data = fetch_income_statement(type_s, code, year, season, '2')
        elif code in ['2841']:
            data = fetch_income_statement(type_s, code, year, season, '2')
        else:
            data = fetch_income_statement(type_s, code, year, season)
        if data:
            data['stock_id'] = stock['id']
            data['stock_date_id'] = stock_date_id
            # 若該股已存在則跳過
            db_lock.acquire()
            income_list = db_mgr.stock_income_statement.read_api(
                s, code=stock['code'], start_date=sn_start_date, end_date=sn_start_date, limit=1)
            if len(income_list) > 0:
                db_mgr.stock_income_statement.update_api(s, oid=income_list[0]['id'], update_data=data)
            else:
                db_mgr.stock_income_statement.create_api(s, data_list=[data])
            db_lock.release()
            if not create_date:
                create_date = current_date
                metadata['income_create_date'] = create_date
            metadata['income_update_date'] = sn_start_date
            db_mgr.stock_metadata.update_api(s, oid=metadata_id, update_data=metadata)
            logger.info(
                "%s(%s)建立 '%s' 的綜合損益表" % (
                    stock['name'], stock['code'], sn_start_date.strftime("%Y-%m-%d")))
        else:
            logger.warning(
                "%s(%s)找不到'%s-%s'綜合損益資料" % (stock['name'], stock['code'], year, season))


class CrawlerTask(threading.Thread):
    def __init__(self, start_date: date = None, end_date: date = None,
                 build_income_table: bool = False, build_balance_table: bool = False,
                 build_revenue_table: bool = False):
        threading.Thread.__init__(self)
        self._session = create_local_session()
        self._date_data = get_stock_dates()
        self._build_income_table = build_income_table
        self._build_balance_table = build_balance_table
        self._build_revenue_table = build_revenue_table
        self._loss_fetch = []
        if not start_date:
            self.start_date = self.default_date()
        else:
            self.start_date = start_date

        if not end_date:
            self.end_date = get_latest_stock_date(self._date_data.get("market_holiday", []))
        else:
            self.end_date = end_date

        # 檢查日期設定
        check_crawler_date_settings(self.start_date, self.end_date, self.default_date())

    @staticmethod
    def default_date() -> date:
        return date(2013, 1, 1)

    def build_balance_sheet_table(self):
        logger.info("build stock balance table")

        # 排除 ETF 相關個股
        etf_list = db_mgr.stock_category.read_api(self._session, name='ETF')
        etf_id = None
        if len(etf_list) == 1:
            etf_id = etf_list[0]['id']

        # 金融股查找需要特定參數
        bank_list = db_mgr.stock_category.read_api(self._session, name='金融保險')
        bank_id = None
        if len(bank_list) == 1:
            bank_id = bank_list[0]['id']

        l_year, l_season = latest_year_season(self.end_date)
        # 取得所有上市股票
        type_list = [t['name'] for t in db_mgr.stock_type.readall_api(self._session)]
        for type_s in type_list:
            stock_list = db_mgr.stock.readall_api(self._session, type_s=type_s, is_alive=True)
            if len(stock_list) == 0:
                logger.warning("資料庫無任何上市股票, 請先取得股票資訊")
                return

            for stock in stock_list:
                code = stock['code']
                # 過濾 ETF 有關的股票
                if etf_id is not None and stock['stock_category_id'] == etf_id:
                    continue

                self.start_date = self.default_date()

                # 檢查 metadata 的日期
                db_lock.acquire()
                metadata_list = db_mgr.stock_metadata.read_api(self._session, code=code)
                if len(metadata_list) == 1:
                    daily_create_date = metadata_list[0]['daily_history_create_date']
                    if daily_create_date and daily_create_date >= self.default_date():
                        _, et = season_range(daily_create_date)
                        st, _ = season_range(et+timedelta(days=1))
                        self.start_date = st
                else:
                    db_mgr.stock_metadata.create_api(self._session, data_list=[{'stock_id': stock['id']}])
                db_lock.release()

                if self.start_date <= self.default_date():
                    _, et = season_range(self.default_date())
                    start_date, _ = season_range(et + timedelta(days=1))
                else:
                    start_date, _ = season_range(self.start_date)

                for current_date in date_range(start_date, self.end_date):
                    # 判斷時間已做到最新的報表
                    year, season = date_to_year_season(current_date)
                    if year >= l_year and season > l_season:
                        break

                    # 只處理每季第一天, 避免重複計算
                    sn_start_date, sn_end_date = season_range(current_date)
                    season_date = sn_start_date
                    if current_date != season_date:
                        continue

                    metadata = dict()
                    metadata_list = db_mgr.stock_metadata.read_api(self._session, code=code)
                    if len(metadata_list) != 1:  # should never run here
                        raise Exception("metadata list is 0")

                    metadata_id = metadata_list[0]['id']
                    create_date = metadata_list[0]['balance_create_date']
                    if not create_date:
                        metadata['balance_create_date'] = sn_start_date

                    # 取得 season date
                    db_lock.acquire()
                    sn_date_list = db_mgr.stock_season_date.read_api(self._session, sn_start_date)
                    if len(sn_date_list) == 0:
                        logger.info("建立 '%s' 的season_date", sn_start_date.strftime("%Y-%m-%d"))
                        db_mgr.stock_season_date.create_api(self._session, data_list=[{'date': sn_start_date}])
                        db_lock.release()
                        sn_date_list = db_mgr.stock_season_date.read_api(self._session, sn_start_date)
                    else:
                        db_lock.release()

                    # 若該股已存在則跳過
                    db_lock.acquire()
                    balance_list = db_mgr.stock_balance_sheet.read_api(
                        self._session, code=stock['code'], start_date=sn_start_date)
                    if len(balance_list) > 0:
                        db_lock.release()
                        continue
                    db_lock.release()

                    # 取得 stock_date_id
                    stock_date_id = sn_date_list[0]['id']

                    # 金融股要特殊參數
                    if bank_id is not None and stock['stock_category_id'] == bank_id:
                        data = fetch_balance_sheet(type_s, code, year, season, '2')
                    elif code in ['2841']:
                        data = fetch_balance_sheet(type_s, code, year, season, '2')
                    else:
                        data = fetch_balance_sheet(type_s, code, year, season)

                    if data:
                        data['stock_id'] = stock['id']
                        data['stock_date_id'] = stock_date_id
                        db_mgr.stock_balance_sheet.create_api(self._session, data_list=[data])
                        metadata['balance_update_date'] = sn_start_date
                        db_mgr.stock_metadata.update_api(self._session, oid=metadata_id, update_data=metadata)
                        logger.info(
                            "%s(%s)建立 '%s' 的資產負債表" % (
                                stock['name'], stock['code'], sn_start_date.strftime("%Y-%m-%d")))
                    else:
                        logger.warning(
                            "%s(%s)找不到'%s-%s'資產負債資料" % (stock['name'], stock['code'], year, season))
                        self._loss_fetch.append(('balance_sheet', stock['code'], year, season))
                        break

    def build_income_statement_table(self):
        logger.info("build stock income statement table")

        # 排除 ETF 相關個股
        etf_list = db_mgr.stock_category.read_api(self._session, 'ETF')
        etf_id = None
        if len(etf_list) == 1:
            etf_id = etf_list[0]['id']

        # 金融股查找需要特定參數
        bank_list = db_mgr.stock_category.read_api(self._session, name='金融保險')
        bank_id = None
        if len(bank_list) == 1:
            bank_id = bank_list[0]['id']

        l_year, l_season = latest_year_season(self.end_date)
        # 取得所有上市股票
        type_list = [t['name'] for t in db_mgr.stock_type.readall_api(self._session)]
        for type_s in type_list:
            stock_list = db_mgr.stock.readall_api(self._session, type_s=type_s, is_alive=True)
            if len(stock_list) == 0:
                logger.warning("資料庫無任何上市股票, 請先取得股票資訊")
                return

            for stock in stock_list:
                code = stock['code']
                # 過濾 ETF 有關的股票
                if etf_id is not None and stock['stock_category_id'] == etf_id:
                    continue

                self.start_date = self.default_date()

                # 檢查 metadata 的日期
                db_lock.acquire()
                metadata_list = db_mgr.stock_metadata.read_api(self._session, code=code)
                if len(metadata_list) == 1:
                    daily_create_date = metadata_list[0]['daily_history_create_date']
                    if daily_create_date and daily_create_date >= self.default_date():
                        _, et = season_range(daily_create_date)
                        st, _ = season_range(et+timedelta(days=1))
                        self.start_date = st
                else:
                    db_mgr.stock_metadata.create_api(self._session, data_list=[{'stock_id': stock['id']}])
                db_lock.release()

                if self.start_date <= self.default_date():
                    _, et = season_range(self.default_date())
                    start_date, _ = season_range(et + timedelta(days=1))
                else:
                    start_date, _ = season_range(self.start_date)

                for current_date in date_range(self.start_date, self.end_date):
                    # 判斷時間已做到最新的報表
                    year, season = date_to_year_season(current_date)
                    if year >= l_year and season > l_season:
                        break

                    # 只處理每季第一天, 避免重複計算
                    sn_start_date, sn_end_date = season_range(current_date)
                    season_date = sn_start_date
                    if current_date != season_date:
                        continue

                    metadata = dict()
                    metadata_list = db_mgr.stock_metadata.read_api(self._session, code=code)
                    if len(metadata_list) != 1:  # should never run here
                        raise Exception("metadata list is 0")
                    metadata_id = metadata_list[0]['id']
                    create_date = metadata_list[0]['income_create_date']
                    if not create_date:
                        metadata['income_create_date'] = sn_start_date

                    # 取得 season date
                    db_lock.acquire()
                    sn_date_list = db_mgr.stock_season_date.read_api(self._session, date=sn_start_date)
                    if len(sn_date_list) == 0:
                        logger.info("建立 '%s' 的season_date", sn_start_date.strftime("%Y-%m-%d"))
                        db_mgr.stock_season_date.create_api(self._session, data_list=[{'date': sn_start_date}])
                        db_lock.release()
                        sn_date_list = db_mgr.stock_season_date.read_api(self._session, date=sn_start_date)
                    else:
                        db_lock.release()

                    # 若該股已存在則跳過
                    db_lock.acquire()
                    income_list = db_mgr.stock_income_statement.read_api(
                        self._session, code=stock['code'], start_date=sn_start_date)
                    if len(income_list) > 0:
                        db_lock.release()
                        continue
                    db_lock.release()

                    # 取得 stock_date_id
                    stock_date_id = sn_date_list[0]['id']

                    # 金融股要特殊參數
                    if bank_id is not None and stock['stock_category_id'] == bank_id:
                        data = fetch_income_statement(type_s, code, year, season, '2')
                    elif code in ['2841']:
                        data = fetch_income_statement(type_s, code, year, season, '2')
                    else:
                        data = fetch_income_statement(type_s, code, year, season)

                    if data:
                        data['stock_id'] = stock['id']
                        data['stock_date_id'] = stock_date_id
                        db_mgr.stock_income_statement.create_api(self._session, data_list=[data])
                        metadata['income_update_date'] = sn_start_date
                        db_mgr.stock_metadata.update_api(self._session, oid=metadata_id, update_data=metadata)
                        logger.info("%s(%s)建立 '%s' 的綜合損益表" % (
                            stock['name'], stock['code'], sn_start_date.strftime("%Y-%m-%d")))
                    else:
                        logger.warning(
                            "%s(%s)找不到'%s-%s'綜合損益資料" % (stock['name'], stock['code'], year, season))
                        self._loss_fetch.append(('income_statement', stock['code'], year, season))
                        break

    def build_monthly_revenue_table(self):
        logger.info("build stock monthly revenue table")

        type_list = [t['name'] for t in db_mgr.stock_type.readall_api(self._session)]
        for type_s in type_list:
            start_date, _ = month_range(self.start_date)
            for current_date in date_range(self.start_date, self.end_date):
                # 判斷時間已做到最新的報表
                if current_date.year >= self.end_date.year and current_date.month > self.end_date.month:
                    break

                # 只處理每月第一天, 避免重複計算
                mn_start_date, mn_end_date = month_range(current_date)
                season_date = mn_start_date
                if current_date != season_date:
                    continue

                # 取得 month date
                db_lock.acquire()
                mn_date_list = db_mgr.stock_monthly_date.read_api(self._session, date=mn_start_date)
                if len(mn_date_list) == 0:
                    logger.info("建立 '%s' 的month_date", mn_start_date.strftime("%Y-%m-%d"))
                    db_mgr.stock_monthly_date.create_api(self._session, data_list=[{'date': mn_start_date}])
                    db_lock.release()
                    mn_date_list = db_mgr.stock_season_date.read_api(self._session, date=mn_start_date)
                else:
                    db_lock.release()

                # 取得 stock_date_id
                stock_date_id = mn_date_list[0]['id']

                data = fetch_monthly_revenue(type_s, mn_start_date.year, mn_start_date.month)
                for code, value in data.items():
                    db_lock.acquire()
                    revenue_list = db_mgr.stock_monthly_revenue.read_api(
                        self._session, code=code, start_date=mn_start_date)
                    if len(revenue_list) > 0:  # 已存在則跳過
                        db_lock.release()
                        continue
                    db_lock.release()

                    stock_list = db_mgr.stock.read_api(self._session, type_s=type_s, code=code)
                    if len(stock_list) == 0:
                        continue

                    # noinspection PyBroadException
                    try:
                        val = int(value.replace(',', ''))
                        revenue = val
                    except Exception:
                        continue

                    name = stock_list[0]['name']
                    data_list = [
                        {'stock_id': stock_list[0]['id'], 'stock_date_id': stock_date_id, 'revenue': revenue}]

                    db_mgr.stock_monthly_revenue.create_api(self._session, data_list=data_list)
                    logger.info("%s(%s)建立 '%s' 的month_date" % (
                        name, code, mn_start_date.strftime("%Y-%m-%d")))

    def run(self):
        logger.info("Starting MOPS thread")
        if self._build_balance_table:
            self.build_balance_sheet_table()
        elif self._build_income_table:
            self.build_income_statement_table()
        elif self._build_revenue_table:
            self.build_monthly_revenue_table()
        else:
            logger.warning("缺少指定建立table的設定參數")

        if len(self._loss_fetch) != 0:
            logger.warning("缺少 %s 的日期資料", self._loss_fetch)
        self._session.close()
        logger.info("Finish MOPS thread")
