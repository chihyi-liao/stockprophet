import click


def my_align(in_str: str, length: int, align_type: str = 'L') -> str:
    """
    中英文混合對齊函式

    :param in_str:[str]輸入的字串
    :param length:[int]對齊長度
    :param align_type:[str]對齊方式（'L'：靠左對齊；'R'：靠右對齊；'C': 置中對齊）
    :return:[str]輸出對齊字串
    """
    str_len = len(in_str)
    zh_count = 0
    for ch in in_str:
        if u'\u4e00' <= ch <= u'\u9fa5':
            str_len += 1
            zh_count += 1

    space = length - str_len - 2 * zh_count
    if align_type == 'L':
        left = ''
        right = ' ' * space + chr(12288) * zh_count
    elif align_type == 'R':
        left = chr(12288) * zh_count + ' ' * space
        right = ''
    else:
        size = space // 2
        zh_size = zh_count // 2
        left = chr(12288) * zh_size + ' ' * size
        right = ' ' * (space - size) + chr(12288) * (zh_count - zh_size)
    return left + in_str + right


def show_result(result: list):
    b_size = 24
    n_size = 16
    s_size = 12
    line = "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}".format(
        my_align('=' * b_size, b_size, 'R'), my_align('=' * s_size, s_size, 'R'),
        my_align('=' * s_size, s_size, 'R'), my_align('=' * s_size, s_size, 'R'),
        my_align('=' * s_size, s_size, 'R'), my_align('=' * s_size, s_size, 'R'),
        my_align('=' * n_size, n_size, 'R'), my_align('=' * n_size, n_size, 'R'))

    header = "{name}\t{price}\t{diff}\t{vol}\t{pbr}\t{eps}\t{op_margin}\t{gross_margin}".format(
        name=my_align("名稱(代號)", b_size, 'R'),
        price=my_align("股價", s_size, 'R'),
        diff=my_align("漲幅(%)", s_size, 'R'),
        vol=my_align("成交量", s_size, 'R'),
        pbr=my_align("淨值比", s_size, 'R'),
        eps=my_align("EPS", s_size, 'R'),
        op_margin=my_align("營益率(%)", n_size, 'R'),
        gross_margin=my_align("毛利率(%)", n_size, 'R'))

    click.echo(line)
    click.echo(header)
    click.echo(line)
    for v in result:
        k = "%s(%s)" % (v[1], v[0])
        msg = "{name}\t{price}\t{diff}\t{vol}\t{pbr}\t{eps}\t{op_margin}\t{gross_margin}".format(
            name=my_align(k, b_size, 'R'),
            price=my_align(str(v[2]), s_size, 'R'),
            diff=my_align(str(v[3])+'%', s_size, 'R'),
            vol=my_align(str(v[4]), s_size, 'R'),
            pbr=my_align(str(v[5]), s_size, 'R'),
            eps=my_align(str(v[6]), s_size, 'R'),
            op_margin=my_align(str(v[7])+'%' if v[7] else '', n_size, 'R'),
            gross_margin=my_align(str(v[8])+'%' if v[8] else '', n_size, 'R'))
        click.echo(msg)


def calc_pbr(code: str, price: float, balance_list: list) -> [None, float]:
    """根據收盤價與資產負債表計算股價淨值比"""
    result = None
    if len(balance_list) == 0:
        return result

    balance = balance_list[0]
    shareholders_net_income = balance.get('shareholders_net_income')
    common_stocks = balance.get('common_stocks')
    total_assets = balance.get('total_assets')
    total_liabs = balance.get('total_liabs')
    special_common_stocks = {'4157': 0.001 * 30, '6548': 1.0, '8070': 1.0}
    par_value = special_common_stocks.get(code, 10.0)
    if common_stocks:
        if shareholders_net_income:
            p = shareholders_net_income / (common_stocks / par_value)  # 每股淨值
            result = round(price / p, 2)  # 股價淨值比
        elif total_assets and total_liabs:
            p = (total_assets - total_liabs) / (common_stocks / par_value)  # 每股淨值
            result = round(price / p, 2)  # 股價淨值比
        else:
            result = None
    return result


def calc_op_margin(income_list: list) -> [None, float]:
    """根據綜合損益表計算營益率"""
    result = None
    if len(income_list) == 0:
        return result

    income = income_list[0]
    net_sales = income['net_sales']
    op_income = income['operating_income']
    if net_sales and op_income:
        result = round((op_income / net_sales) * 100.0, 2)
    return result


def calc_gross_margin(income_list: list) -> [None, float]:
    """根據綜合損益表計算毛利率"""
    result = None
    if len(income_list) == 0:
        return result

    income = income_list[0]
    net_sales = income['net_sales']
    gross_profit = income['gross_profit']
    if net_sales and gross_profit:
        result = round((gross_profit / net_sales) * 100.0, 2)
    return result


def calc_eps(income_list: list) -> [None, float]:
    """根據綜合損益表計算EPS"""
    result = None
    if len(income_list) == 0:
        return result

    income = income_list[0]
    result = income['eps']
    return result


def is_liabs_desc(balance_list: list, n: int) -> bool:
    """連續 n 季負債減少"""
    result = False
    if len(balance_list) != n:
        return result

    value = 0
    for balance in balance_list:
        liabs = balance['total_liabs']
        if not liabs:
            continue

        if value == 0:
            value = liabs
        else:
            if liabs < value:
                result = False
                break
            else:
                value = liabs
                result = True

    return result


def is_asserts_asc(balance_list: list, n: int) -> bool:
    """連續 n 季資產增加"""
    result = False
    if len(balance_list) != n:
        return result

    value = 0
    for balance in balance_list:
        asserts = balance['total_assets']
        if not asserts:
            continue

        if value == 0:
            value = asserts
        else:
            if asserts > value:
                result = False
                break
            else:
                value = asserts
                result = True

    return result