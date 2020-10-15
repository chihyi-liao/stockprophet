from datetime import date

Kilo = 1000
Fee = 1.425 / Kilo
Tax = 3.0 / Kilo


class Account(object):
    def __init__(self, principal: int):
        self.records = []
        self.principal: int = principal
        self.total_assets: int = principal
        self.avg_price: float = 0.0
        self.stock_assets: int = 0
        self.total_volume: int = 0

    def clear(self):
        self.avg_price: float = 0.0
        self.stock_assets: int = 0
        self.total_volume: int = 0

    def get_avg_price(self):
        return self.avg_price

    def get_total_volume(self):
        return self.total_volume

    def get_records(self):
        return self.records

    def get_roi(self, price: float):
        cost = price * self.total_volume * Kilo
        avg_cost = self.avg_price * self.total_volume * Kilo
        return round((cost - avg_cost) * 100 / self.stock_assets, 2)

    def buy(self, dt: date, price: float, volume: int):
        stock_cost = price * volume * Kilo
        stock_fee = int(stock_cost * Fee)
        if volume and self.principal - (stock_cost + stock_fee) > 0:
            self.stock_assets += stock_cost
            self.principal -= (stock_cost + stock_fee)
            self.total_volume += volume
            self.avg_price = self.get_buy_avg_price(price, volume)
            self.total_assets = self.principal + self.stock_assets
            self.records.append([
                dt.strftime("%Y-%m-%d"), round(price, 2), round(volume, 2),
                None, None, self.avg_price, self.total_volume, int(self.stock_assets),
                int(self.principal), int(self.total_assets), self.get_roi(price)])

    def get_max_buy_volume(self, price: float):
        volume = int(self.principal / price * Kilo)
        cost = price * volume * Kilo + int(price * volume * Kilo * Fee)
        if cost > self.principal:
            volume = volume - 1
        return volume

    def get_buy_avg_price(self, price: float, volume: int):
        buy_cost = price * volume * Kilo + (price * volume * Kilo * Fee)
        avg_cost = self.avg_price * (self.total_volume - volume) * Kilo
        return round((buy_cost + avg_cost) / (self.total_volume * Kilo), 2)

    def sell(self, dt: date, price: float, volume: int):
        if 0 < volume <= self.total_volume:
            stock_cost = volume * price * Kilo
            stock_tax = int(stock_cost * Tax)
            avg_price = self.get_sell_avg_price(price, volume)
            roi = self.get_roi(price)
            self.stock_assets -= stock_cost
            self.principal += (stock_cost - stock_tax)
            self.total_volume -= volume
            self.avg_price = avg_price
            if self.total_volume == 0:
                avg_price = 0.0
                self.clear()
            self.total_assets = self.principal + self.stock_assets

            self.records.append([
                dt.strftime("%Y-%m-%d"), None, None,
                round(price, 2), round(volume, 2),
                avg_price, self.total_volume, int(self.stock_assets),
                int(self.principal), int(self.total_assets), roi])

    def get_sell_avg_price(self, price: float, volume: int):
        sell_cost = price * volume * Kilo - (price * volume * Kilo * Tax)
        if self.total_volume - volume == 0:
            avg_cost = 0
        else:
            avg_cost = self.avg_price * (self.total_volume - volume) * Kilo
        return round((sell_cost + avg_cost) / (self.total_volume * Kilo), 2)
