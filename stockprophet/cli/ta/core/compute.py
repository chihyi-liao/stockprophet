import numpy as np


def sma(values, n):
    cumulate_sum = np.cumsum(np.insert(values, 0, 0))
    return (cumulate_sum[n:] - cumulate_sum[:-n]) / float(n)


def ewma(values, n):
    result = []
    for val in values:
        i = len(result)
        if i < n:
            result += [float(sum(result) + val) / (i + 1)]
        else:
            last = result[-1]
            result += [last + 2. * (val - last) / (n + 1)]
    return result


def macd(values, fast=12, slow=26, n=9):
    ema_fast = np.array(ewma(values, fast))
    ema_slow = np.array(ewma(values, slow))
    macd_ = ema_fast - ema_slow
    signal = np.array(ewma(macd_, n))
    diff = macd_ - signal
    return np.around(macd_, decimals=6), np.around(signal, decimals=6), np.around(diff, decimals=6)


def rsv(high_values, low_values, close_values, n=9):
    result = []
    for i, value in enumerate(close_values, start=1):
        if i < n:
            continue
        n_low = min(low_values[i - n:i])
        n_high = max(high_values[i - n:i])
        if n_high - n_low == 0:
            continue
        result += [100. * (value - n_low) / (n_high - n_low)]
    return result


def kdj(high_values, low_values, close_values, n=9):
    data = rsv(high_values, low_values, close_values, n)
    k_data = []
    d_data = []
    j_data = []
    for i, val in enumerate(data):
        k = 50.0 if len(k_data) == 0 else k_data[i - 1]
        d = 50.0 if len(d_data) == 0 else d_data[i - 1]
        k_data += [(2 / 3 * k) + (1 / 3 * val)]
        d_data += [(2 / 3 * d) + (1 / 3 * k_data[-1])]
        j_data += [(3 * d_data[-1]) - (2 * k_data[-1])]
    return k_data, d_data, j_data
