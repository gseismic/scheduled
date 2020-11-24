# import arrow
import json
import time
import scheduled
from finapi.eastmoney import EmNoticeAPI


class Fetcher(scheduled.Fetcher):

    def __init__(self, *args, **kwargs):
        super(Fetcher, self).__init__(*args, **kwargs)
        self.api = EmNoticeAPI()
        self.api.spider.retry_count = self.config.get('retry_count', 10)
        self.api.spider.timeout = self.config.get('timeout', 20)
        self.interval = self.config.get('interval', 3.0)

    def fetch(self, key):
        code, date, _info = key.split('=')
        info = json.loads(_info)

        params = dict(StockCode=code, FirstNodeType=info['first_code'],
                      SecNodeType=info['second_code'], publish_date=date)
        data = self._get_all_notices(**params)
        options = {}
        options.update(info)
        return data, options

    def _get_all_notices(self, StockCode, FirstNodeType, 
                        SecNodeType, publish_date):
        data_list = []
        page, pagesize = 1, 5000
        while True:
            self.logger.info('page %d ..' % page)
            data = self.api.get_notice_simple(StockCode=StockCode,
                                              FirstNodeType=FirstNodeType, 
                                              SecNodeType=SecNodeType,
                                              page=page, pagesize=pagesize,
                                              publish_date=publish_date)
            if page == 1:
                self.logger.info('Count: %d, pages: %s' % (data['TotalCount'], data['pages']))
            data_list.extend(data['data'])
            page += 1
            if page > data['pages']:
                assert(len(data_list) == data['TotalCount'])
                break
            time.sleep(self.interval)
        return data_list
