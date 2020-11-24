import time
from .. import scheduled
from finapi.sina import SinaFinanceAPI
from finapi.common.spider import FreeSpider, SpiderClient


class Fetcher(scheduled.Fetcher):

    def __init__(self, *args, **kwargs):
        super(Fetcher, self).__init__(*args, **kwargs)
        local = False
        if local:
            self.retry_count = self.config.get('retry_count', 3)
            self.timeout = self.config.get('timeout', 10)
            self.spider = FreeSpider(logger=self.logger, 
                                retry_count=self.retry_count,
                                timeout=self.timeout)
        else:
            server_list = self.config['server_list']
            self.spider = SpiderClient(server_list, logger=self.logger)
        self.api = SinaFinanceAPI(spider=self.spider, logger=self.logger)
        self.fetch_interval = self.config.get('fetch_interval', 0.3)

    def fetch(self, key):
        # 只要有一个不成功就会raise
        type_, symbol = key.split(':')
        assert(symbol[:2] in ['SH', 'SZ'])
        code = symbol[2:]

        options, data = {}, {}

        self.api.spider.random_switch_server()
        cashflow = self.api.get_cashflow_statement(code)
        time.sleep(self.fetch_interval)

        self.api.spider.random_switch_server()
        balance = self.api.get_balance_sheet(code)
        time.sleep(self.fetch_interval)

        self.api.spider.random_switch_server()
        income = self.api.get_income_statement(code)
        time.sleep(self.fetch_interval)

        data = {'cashflow': cashflow, 'balance': balance, 'income': income}
        options = {'symbol': symbol}
        return data, options
