import abc
import time
import json
import random
import logging
import traceback
import requests
import threading


class BaseProxyProvider:

    @abc.abstractmethod
    def fetchone(self):
        raise NotImplementedError()


_logger = logging.getLogger(__name__)
class ProxyProvider(BaseProxyProvider):

    DEFAULT_PROXY_API = 'http://106.54.201.205:5089/good_proxy'

    def __init__(self, proxy_api=None, 
                 max_response_time=5.9,
                 high_anonymity_only=True,
                 fetch_interval=60):
        self.proxy_api = proxy_api or self.DEFAULT_PROXY_API
        self.max_response_time = max(1.0, min(max_response_time, 20))
        self.high_anonymity_only = high_anonymity_only
        self.fetch_interval = fetch_interval

        self.proxy_list = []
        self._lock = threading.RLock()
        _logger.info('Start proxy updater ...')
        self._thread_update = threading.Thread(target=self.task_update)
        self._thread_update.daemon = True
        self._thread_update.start()
        time.sleep(5)
        # self.good_proxy_set = set()

    def clear_proxy(self):
        self.proxy_list = []

    def task_update(self):
        while 1:
            try:
                r = requests.get(self.proxy_api)
                data = json.loads(r.text)
                with self._lock:
                    self.clear_proxy()
                    for item in data['data']:
                        if item['proxy'].startswith('127.'):
                            continue
                        if self.high_anonymity_only and item['x_anonymity'] not in ['H']:
                            continue
                        if item['x_response_time'] > self.max_response_time:
                            continue
                        # print(item)
                        self.proxy_list.append(item['proxy'])
            except Exception:
                _logger.error('Error fetching proxy from api')
                _logger.error(traceback.format_exc())
            time.sleep(self.fetch_interval)

    def fetchone(self):
        rv = None
        with self._lock:
            if self.proxy_list:
                random.shuffle(self.proxy_list)
                rv = self.proxy_list[0]
        return rv


class ProxySpider:

    def __init__(self, spider, proxy_provider=None, 
                 retry_count=3, retry_sleep=0.5, logger=None):
        self.spider = spider
        self.proxy_provider = proxy_provider or ProxyProvider()
        self.retry_count = retry_count
        self.retry_sleep = retry_sleep
        self.logger = logger or logging.getLogger(__name__)
        assert(hasattr(self.proxy_provider, 'fetchone'))
        assert(hasattr(spider, 'get_source'))
        assert(hasattr(spider, 'post_source'))

    def switch_proxy(self, proxy):
        if isinstance(proxy, dict) or proxy is None:
            self.spider.proxies = proxy
        else:
            if not proxy.startswith('http'):
                http = 'http://' + proxy
                https = 'https://' + proxy
            else:
                http = https = proxy
            # set default proxies
            self.spider.proxies = {'http': http, 'https': https}

    def get_source(self, *args, **kwargs):
        return self.xxx_source('GET', self.spider.get_source, *args, **kwargs)

    def post_source(self, *args, **kwargs):
        return self.xxx_source('POST', self.spider.post_source, *args, **kwargs)

    def xxx_source(self, name, callback, *args, **kwargs):
        countdown = self.retry_count
        while countdown > 0:
            try:
                proxy = self.proxy_provider.fetchone()
                self.switch_proxy(proxy)
                self.logger.info('Using %s ...[%s]' % (proxy, countdown))
                return callback(*args, **kwargs)
            except Exception as e:
                # traceback.print_exc()
                # logging.info(str(e) + '.. retry ..')
                pass
            time.sleep(self.retry_sleep)
            countdown -= 1
        raise Exception('MaxRetryNum(%s)' % name)


class IpDetector:

    def detect(self, spider):
        url = 'http://ip.taobao.com/outGetIpInfo'
        payload = {'ip': 'myip', 'accessKey': 'alibaba-inc'}
        text = spider.post_source(url, data=payload)
        return json.loads(text)

    def detect2(self, spider, proxies):
        url = 'http://ip.taobao.com/outGetIpInfo'
        payload = {'ip': 'myip', 'accessKey': 'alibaba-inc'}
        text = spider.post_source(url, data=payload)
        return json.loads(text)


def test_old():
    from finapi.common.spider import FreeSpider
    provider = ProxyProvider()
    spider = ProxySpider(FreeSpider(retry_count=1))
    while 1:
        try:
            proxy = provider.fetchone()
            print('Using %s ...' % proxy)
            spider.switch_proxy(proxy)
            print(IpDetector().detect(spider))
        except Exception:
            traceback.print_exc()
        time.sleep(3.0)


if __name__ == '__main__':
    from finapi.common.spider import FreeSpider
    logging.basicConfig(level=logging.DEBUG) # 第一次配置生效，其余无效
    # logging.basicConfig(level=logging.INFO)

    pspider = ProxySpider(FreeSpider(retry_count=0, timeout=5), retry_count=12)
    url = 'http://ip.taobao.com/outGetIpInfo'
    payload = {'ip': 'myip', 'accessKey': 'alibaba-inc'}
    while 1:
        text = pspider.post_source(url, data=payload)
        print(text)
        time.sleep(1)
