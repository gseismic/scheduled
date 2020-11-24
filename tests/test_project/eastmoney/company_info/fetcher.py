# import arrow
import scheduled
from finapi.eastmoney import EmF10API
from finapi.common.spider import FreeSpider
from .proxy_spider import ProxySpider, ProxyProvider


class Fetcher(scheduled.Fetcher):

    def __init__(self, *args, **kwargs):
        super(Fetcher, self).__init__(*args, **kwargs)
        proxy_api = 'http://106.54.201.205:5089/good_proxy'
        provider = ProxyProvider(proxy_api=proxy_api)

        retry_count = 12 # self.config.get('retry_count', 12)
        timeout = self.config.get('timeout', 10)
        self.spider = ProxySpider(
            FreeSpider(retry_count=0, timeout=timeout), 
            proxy_provider=provider,
            retry_count=retry_count, retry_sleep=0.1,
            logger=self.logger
        )
        self.api = EmF10API(spider=self.spider, logger=self.logger)

    def fetch(self, key):
        symbol = key.upper()
        data = self.api.get_company_info(symbol)

        if self.key_of_result(data) != key.upper():
            raise Exception('CacheException')

        options = {'symbol': symbol}
        return data, options

    def key_of_result(self, data):
        return data['Market'] + data['SecurityCode']
