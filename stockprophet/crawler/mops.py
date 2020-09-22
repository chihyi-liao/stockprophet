import random
import time
import threading
from datetime import date
from typing import Optional

from lxml import html

from stockprophet.db import create_local_session, db_lock
from stockprophet.db import manager as db_mgr
from stockprophet.utils import get_logger
from .utils.date import (
    get_latest_stock_date, season_range, latest_year_season, date_to_year_season,
    date_range, check_crawler_date_settings
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
        '資本公積合計': 'capital_reserve', '普通股股本': 'common_stocks',
        '股本合計': 'total_stocks', '存貨': 'inventories', '預付款項': 'prepaid',
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
        if 'overrun' in item.lower():
            is_overrun = True
        break
    if is_overrun:
        logger.info("爬蟲降速: 查詢過載")
        return True
    else:
        return False


def fetch_monthly_revenue(type_s: str, code: str, year: int, month: int, retry: int = 3):
    """
    取得月營收資料可以從2013/01到現在
    """
    req = HttpRequest()
    url = "{base_url}/ajax_t05st10_ifrs".format(base_url=MOPS_URL)
    kwargs = dict()
    kwargs['headers'] = req.form_headers()
    kwargs['data'] = {'encodeURIComponent': '1',
                      'step': '1',
                      'firstin': '1',
                      'co_id': code,
                      'year': year - 1911,
                      'month': "%02d" % month}
    result = {}
    stock_type = convert_stock_type(type_s)
    if not stock_type:
        return result

    req.wait_interval = random.randint(1, 5)
    kwargs['data']['TYPEK'] = stock_type
    for i in range(retry):
        resp = req.send_data('POST', url, **kwargs)
        if resp.status_code == 200:
            tree = html.fromstring(resp.text)
            root = "/html/body/table[@class='hasBorder']"
            if check_over_run(tree):
                req.wait_interval = random.randint(20, 30)
                logger.warning("股市代號: %s, 無法取得%s-%s月營收報表資料(過載)", code, year, month)
                continue
            else:
                # 解析當月營收
                req.wait_interval = random.randint(1, 5)
                subtree = tree.xpath("{root}/tr".format(root=root))
                for item in subtree:
                    tbl_heads = item.xpath("th[@class='tblHead']/text()")
                    if '本月' in tbl_heads:
                        values = item.xpath("td/text()")
                        if len(values):
                            revenue = "".join(values[0].split())
                            try:
                                result['revenue'] = int(revenue.replace(',', ''))
                            except (ValueError, TypeError):
                                result['revenue'] = None
                # 解析當月營收備註
                subtree = tree.xpath("{root}/th".format(root=root))
                for item in subtree:
                    notes = item.xpath("text()")
                    if len(notes) and '備註' in notes:
                        values = tree.xpath(
                            "{root}/td/pre/text()".format(root=root))
                        if len(values):
                            result['notes'] = "".join(values[0].split())
                break
        else:
            logger.warning("股市代號: %s, 無法取得%s-%s月營收報表資料" % (code, year, month))
    return result


def fetch_income_statement(type_s: str, code: str, year: int, season: int, retry: int = 3):
    """Income statement is started from 2013/01 to now, which is based on IFRSs policy
    """
    req = HttpRequest()
    url = "{base_url}/ajax_t164sb04".format(base_url=MOPS_URL)
    kwargs = dict()
    kwargs['headers'] = req.form_headers()
    kwargs['data'] = {'encodeURIComponent': '1',
                      'step': '1',
                      'firstin': '1',
                      'co_id': code,
                      'year': year - 1911,
                      'season': "%02d" % season}
    result = {}
    stock_type = convert_stock_type(type_s)
    if not stock_type:
        return result

    req.wait_interval = random.randint(1, 5)
    kwargs['data']['TYPEK'] = stock_type
    for i in range(retry):
        resp = req.send_data('POST', url, **kwargs)
        if resp.status_code == 200:
            tree = html.fromstring(resp.text)
            root = "/html/body/center/table[@class='hasBorder']"
            if check_over_run(tree):
                req.wait_interval = random.randint(20, 30)
                logger.warning("股市代號: %s, 無法取得%s-%Qs綜合損益表資料(過載)", code, year, season)
                continue
            else:
                # 解析綜合損益表
                req.wait_interval = random.randint(1, 5)
                subtree = tree.xpath("{root}/tr".format(root=root))
                for item in subtree:
                    titles = item.xpath(
                        "td[@style='text-align:left;white-space:nowrap;']/text()")
                    titles = ["".join(r.split()) for r in titles]
                    raw_data = item.xpath("td[@style='text-align:right;']/text()")
                    values = ["".join(r.split()) for r in raw_data]
                    if len(titles) == 1 and len(values) > 0:
                        result[titles[0]] = values[0]
                break
        else:
            logger.warning("股市代號: %s, 無法取得%s-Q%s綜合損益表資料", code, year, season)
    return translate_income_statement(result)


def fetch_balance_sheet(type_s: str, code: str, year: int, season: int, retry: int = 3):
    """Balance sheet is started from 2013/01 to now, which is based on IFRSs policy
    """
    req = HttpRequest()
    url = "{base_url}/ajax_t164sb03".format(base_url=MOPS_URL)
    kwargs = dict()
    kwargs['headers'] = req.form_headers()
    kwargs['data'] = {'encodeURIComponent': '1',
                      'step': '1',
                      'firstin': '1',
                      'co_id': code,
                      'year': year - 1911,
                      'season': "%02d" % season}
    result = {}
    stock_type = convert_stock_type(type_s)
    if not stock_type:
        return result

    req.wait_interval = random.randint(1, 5)
    kwargs['data']['TYPEK'] = stock_type
    for i in range(retry):
        resp = req.send_data('POST', url, **kwargs)
        if resp.status_code == 200:
            tree = html.fromstring(resp.text)
            if check_over_run(tree):
                req.wait_interval = random.randint(20, 30)
                logger.warning("股市代號: %s, 無法取得%s-Q%s資產負債表資料(過載)", code, year, season)
                continue
            else:
                req.wait_interval = random.randint(1, 5)
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
                break
        else:
            logger.warning("股市代號: %s, 無法取得%s-Q%s資產負債表資料", code, year, season)
    return translate_balance_sheet(result)


class CrawlerTask(threading.Thread):
    def __init__(self, start_date: date = None, end_date: date = None,
                 build_income_table: bool = False, build_balance_table: bool = False):
        threading.Thread.__init__(self)
        default_date = date(2013, 1, 1)

        self._session = create_local_session()
        self._date_data = get_stock_dates()
        self._build_income_table = build_income_table
        self._build_balance_table = build_balance_table
        self._loss_fetch = []
        if not start_date:
            self.start_date = default_date
        else:
            self.start_date = start_date

        if not end_date:
            self.end_date = get_latest_stock_date(self._date_data.get("market_holiday", []))
        else:
            self.end_date = end_date

        # 檢查日期設定
        check_crawler_date_settings(self.start_date, self.end_date, default_date)

    def build_balance_sheet_table(self):
        logger.info("build stock balance table")
        etf_list = db_mgr.stock_category.read_api(self._session, 'ETF')
        etf_id = None
        if len(etf_list) == 1:
            etf_id = etf_list[0]['id']
        l_year, l_season = latest_year_season(self.end_date)

        # 取得所有上市股票
        type_list = [t['name'] for t in db_mgr.stock_type.readall_api(self._session)]
        for type_s in type_list:
            stock_list = db_mgr.stock.readall_api(self._session, type_s=type_s, is_alive=True)
            if len(stock_list) == 0:
                logger.warning("資料庫無任何上市股票, 請先取得股票資訊")
                return

            for _j, stock in enumerate(stock_list, start=1):
                code = stock['code']
                # 過濾 ETF 有關的股票
                if etf_id is not None and stock['stock_category_id'] == etf_id:
                    continue

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

                    data = fetch_balance_sheet(type_s, code, year, season)
                    if data:
                        data['stock_id'] = stock['id']
                        data['stock_date_id'] = stock_date_id
                        logger.info(
                            "%s(%s)建立 '%s' 的season_date" % (
                                stock['name'], stock['code'], sn_start_date.strftime("%Y-%m-%d")))
                        db_mgr.stock_balance_sheet.create_api(self._session, data_list=[data])
                    else:
                        logger.warning(
                            "%s(%s)找不到'%s-%s'資產負債資料" % (stock['name'], stock['code'], year, season))
                        self._loss_fetch.append(('balance_sheet', stock['code'], year, season))

        # 更新 metadata
        start_dt, _ = season_range(self.end_date)
        metadata = db_mgr.stock_metadata.read_api(self._session)
        if len(metadata) == 1:
            db_mgr.stock_metadata.update_api(
                self._session, metadata[0]['id'],
                update_data={'mops_balance_sheet_update_date': start_dt})

    def build_income_statement_table(self):
        logger.info("build stock income statement table")
        etf_list = db_mgr.stock_category.read_api(self._session, 'ETF')
        etf_id = None
        if len(etf_list) == 1:
            etf_id = etf_list[0]['id']
        l_year, l_season = latest_year_season(self.end_date)

        # 取得所有上市股票
        type_list = [t['name'] for t in db_mgr.stock_type.readall_api(self._session)]
        for type_s in type_list:
            stock_list = db_mgr.stock.readall_api(self._session, type_s=type_s, is_alive=True)
            if len(stock_list) == 0:
                logger.warning("資料庫無任何上市股票, 請先取得股票資訊")
                return

            for _j, stock in enumerate(stock_list, start=1):
                # 過濾 ETF 有關的股票
                if etf_id is not None and stock['stock_category_id'] == etf_id:
                    continue

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
                    income_list = db_mgr.stock_income_statement.read_api(
                        self._session, code=stock['code'], start_date=sn_start_date)
                    if len(income_list) > 0:
                        db_lock.release()
                        continue
                    db_lock.release()

                    # 取得 stock_date_id
                    stock_date_id = sn_date_list[0]['id']

                    data = fetch_income_statement(type_s, stock['code'], year, season)
                    if data:
                        data['stock_id'] = stock['id']
                        data['stock_date_id'] = stock_date_id
                        logger.info("%s(%s)建立 '%s' 的season_date" % (
                            stock['name'], stock['code'], sn_start_date.strftime("%Y-%m-%d")))
                        db_mgr.stock_income_statement.create_api(self._session, data_list=[data])
                    else:
                        logger.warning(
                            "%s(%s)找不到'%s-%s'綜合損益資料" % (stock['name'], stock['code'], year, season))
                        self._loss_fetch.append(('income_statement', stock['code'], year, season))
        # 更新 metadata
        start_dt, _ = season_range(self.end_date)
        metadata = db_mgr.stock_metadata.read_api(self._session)
        if len(metadata) == 1:
            db_mgr.stock_metadata.update_api(
                self._session, metadata[0]['id'],
                update_data={'mops_income_statement_update_date': start_dt})

    def run(self):
        logger.info("Starting MOPS thread")
        while True:
            # try:
            metadata = db_mgr.stock_metadata.read_api(self._session)
            if len(metadata) == 1:
                tse_stock_update_date = metadata[0]['tse_stock_info_update_date']
                otc_stock_update_date = metadata[0]['otc_stock_info_update_date']
                income_update_date = metadata[0]['mops_income_statement_update_date']
                balance_update_date = metadata[0]['mops_balance_sheet_update_date']
                if tse_stock_update_date or otc_stock_update_date:  # 確認資料庫有個股資訊
                    if self._build_balance_table:
                        if balance_update_date:
                            self.start_date = balance_update_date
                        self.build_balance_sheet_table()
                    elif self._build_income_table:
                        if income_update_date:
                            self.start_date = income_update_date
                        self.build_income_statement_table()
                    else:
                        logger.warning("缺少指定建立table的設定參數")
                    if len(self._loss_fetch) != 0:
                        logger.warning("缺少 %s 的日期資料", self._loss_fetch)
                    self._session.close()
                    break
            time.sleep(30)
            # except Exception as e:
            #     logger.error(str(e))
            #     break
        logger.info("Finish MOPS thread")
