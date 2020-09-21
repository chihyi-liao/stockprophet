from stockprophet.db import get_session, db_lock
from stockprophet.db import manager as db_mgr


STOCK_DATES_URL = "https://raw.githubusercontent.com/chihyi-liao/stock-data/master/date_data.json"
STOCK_TYPES = ['tse', 'otc']
STOCK_CATEGORIES = [
    '水泥工業', '食品工業', '塑膠工業', '紡織纖維',
    '電機機械', '電器電纜', '玻璃陶瓷', '造紙工業',
    '鋼鐵工業', '橡膠工業', '汽車工業', '建材營造',
    '航運業', '觀光事業', '金融保險', '貿易百貨',
    '其他', '化學工業', '生技醫療業', '油電燃氣業',
    '半導體業', '電腦及週邊設備業', '光電業', '通信網路業',
    '電子零組件業', '電子通路業', '資訊服務業', '其他電子業',
    'ETF', '文化創意業', '農業科技業', '電子商務業', '未分類']


def init_stock_type():
    data_list = []
    s = get_session()
    db_lock.acquire()
    for name in STOCK_TYPES:
        if len(db_mgr.stock_type.read_api(s, name)) == 0:
            data_list.append({'name': name})
    if data_list:
        db_mgr.stock_type.create_api(s, data_list)
    db_lock.release()
    s.close()


def init_stock_category():
    data_list = []
    s = get_session()
    db_lock.acquire()
    for name in STOCK_CATEGORIES:
        if len(db_mgr.stock_category.read_api(s, name)) == 0:
            data_list.append({'name': name})
    if data_list:
        db_mgr.stock_category.create_api(s, data_list)
    db_lock.release()
    s.close()


def init_stock_metadata():
    data_list = [{
        'tse_stock_info_update_date': None,
        'tse_weekly_history_update_date': None,
        'tse_monthly_history_update_date': None,
        'tse_mops_update_date': None,
        'otc_stock_info_update_date': None,
        'otc_weekly_history_update_date': None,
        'otc_monthly_history_update_date': None,
        'otc_mops_update_date': None}]
    s = get_session()
    db_lock.acquire()
    if len(db_mgr.stock_metadata.read_api(s)) == 0:
        db_mgr.stock_metadata.create_api(s, data_list)
    db_lock.release()
    s.close()
