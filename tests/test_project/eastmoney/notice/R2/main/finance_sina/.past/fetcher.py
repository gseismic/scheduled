# import arrow
import time
from .. import scheduled
from finapi.sina import SinaQuoteAPI


class Fetcher(scheduled.Fetcher):

    def __init__(self, *args, **kwargs):
        super(Fetcher, self).__init__(*args, **kwargs)
        self.api = SinaQuoteAPI()
        self.fetcher_interval = self.config.get('fetcher_interval', 1)
        self.retry_count = self.config.get('retry_count', 3)
        self.timeout = self.config.get('timeout', 10)

    def fetch(self, key):
        items = key.split(':')
        type_, symbol = items[0], items[1]

        #t0 = time.time()
        qfq = self.api.get_all_qfq_factors(symbol)
        # self.logger.debug('fetch time1 %f' % (time.time() - t0))
        #print('qfq fetch time1 %f' % (time.time() - t0))

        time.sleep(self.fetcher_interval)
        #t0 = time.time()
        hfq = self.api.get_all_hfq_factors(symbol)
        #print('hfq fetch time2 %f' % (time.time() - t0))

        # 访问失败会直接触发Exception
        assert(qfq['total'] == hfq['total'])
        assert(len(qfq['data']) == len(hfq['data']))
        qfq_data = sorted(qfq['data'], key=lambda x: x['d'], reverse=False)
        hfq_data = sorted(hfq['data'], key=lambda x: x['d'], reverse=False)

        # data = {'qfq': qfq_data, 'hfq': hfq_data}
        data = []
        num = len(qfq_data)
        for i in range(num):
            assert(qfq_data[i]['d'] == hfq_data[i]['d'])
            dic = {'date': qfq_data[i]['d'],
                   'qfq': qfq_data[i]['f'],
                   'symbol': symbol.upper(),
                   'hfq': hfq_data[i]['f']}
            data.append(dic)

        options = {}
        options['type'] = type_
        options['symbol'] = symbol
        return data, options
