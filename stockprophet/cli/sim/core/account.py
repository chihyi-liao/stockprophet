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
        """取得投資報酬率"""
        cost = price * self.total_volume
        avg_cost = self.avg_price * self.total_volume
        return round((cost - avg_cost) * 100 / self.stock_assets, 2)

    def buy(self, dt: date, price: float, volume: int):
        """執行買股"""
        stock_cost = price * volume
        stock_fee = int(stock_cost * Fee)
        if volume and self.principal - (stock_cost + stock_fee) > 0:
            self.principal -= (stock_cost + stock_fee)
            self.total_volume += volume
            self.avg_price = self.get_buy_avg_price(price, volume)
            self.stock_assets = price * self.total_volume
            self.total_assets = int(self.principal + self.stock_assets)
            self.records.append([
                dt.strftime("%Y-%m-%d"), round(price, 2), volume,
                None, None, self.avg_price, self.total_volume, int(self.stock_assets),
                int(self.principal), int(self.total_assets), self.get_roi(price)])

    def get_max_buy_volume(self, price: float):
        """取得最大買量(單位:張數)"""
        volume = int(self.principal / (price * Kilo))
        cost = price * volume * Kilo + int(price * volume * Kilo * Fee)
        if cost > self.principal:
            volume = volume - 1
        return volume * Kilo

    def get_buy_avg_price(self, price: float, volume: int):
        """取得買股之後的平均價"""
        buy_cost = price * volume + (price * volume * Fee)
        avg_cost = self.avg_price * (self.total_volume - volume)
        return round((buy_cost + avg_cost) / self.total_volume, 2)

    def sell(self, dt: date, price: float, volume: int):
        """執行賣股"""
        if 0 < volume <= self.total_volume:
            stock_cost = volume * price
            stock_tax = int(stock_cost * Tax)
            avg_price = self.get_sell_avg_price(price, volume)
            roi = self.get_roi(price)
            self.principal += (stock_cost - stock_tax)
            self.total_volume -= volume
            self.avg_price = avg_price
            self.stock_assets = price * self.total_volume
            if self.total_volume == 0:
                avg_price = 0.0
                self.clear()
            self.total_assets = int(self.principal + self.stock_assets)

            self.records.append([
                dt.strftime("%Y-%m-%d"), None, None,
                round(price, 2), volume, avg_price,
                self.total_volume, int(self.stock_assets),
                int(self.principal), int(self.total_assets), roi])

    def get_sell_avg_price(self, price: float, volume: int):
        """取得賣股之後的平均價"""
        sell_cost = price * volume - (price * volume * Tax)
        if self.total_volume - volume == 0:
            avg_cost = 0
        else:
            avg_cost = self.avg_price * (self.total_volume - volume)
        return round((sell_cost + avg_cost) / self.total_volume, 2)

    def calculate_reduction(self, dt: date,
                            new_kilo_stock: float, give_back_per_stock: float,
                            old_price: float, new_price: float):
        """計算減資後的行情"""
        if self.total_volume:
            volume = self.total_volume - int(self.total_volume * (new_kilo_stock / Kilo))
            stock_cost = volume * give_back_per_stock
            stock_tax = int(stock_cost * Tax)
            avg_price = round(self.avg_price * (new_price / old_price), 2)
            self.principal += (stock_cost - stock_tax)
            self.total_volume -= volume
            self.stock_assets = new_price * self.total_volume
            self.avg_price = avg_price
            self.total_assets = int(self.principal + self.stock_assets)
            roi = self.get_roi(new_price)
            self.records.append([
                '* %s' % (dt.strftime("%Y-%m-%d"), ), None, None,
                round(new_price, 2), volume,
                avg_price, self.total_volume, int(self.stock_assets),
                int(self.principal), int(self.total_assets), roi])
