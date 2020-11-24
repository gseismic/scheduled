# coding: utf8
import json
import arrow
import scheduled
from finapi.eastmoney import EmNoticeAPI


class OnlineKeyYielder(scheduled.KeyYielder):

    def __init__(self, config=None, logger=None):
        super(OnlineKeyYielder, self).__init__(config, logger)
        self.api = EmNoticeAPI(logger=self.logger)

        assert(self.config['mode'].lower() in ['update', 'fix'])
        if self.config['mode'].lower() == 'fix':
            self.logger.notice('Using `once` mode ...')
            self.is_update_mode = False
        else:
            self.logger.notice('Using `update` mode ...')
            self.is_update_mode = True
            now = arrow.now()
            self.start_date = now.shift(days=-abs(self.config['days_to_today']))
            self.end_date = now

        self.__all_keys = self.get_all_keys()
        self.__i = 0

    def get_all_keys(self):
        all_keys = []
        nodes = self.api.get_category_info()
        symbol = ''
        for node in nodes:
            for item in node['second']:
                # 跳过【全部】分类 0
                if len(node['second']) >= 2 and item['second_code'] == 0:
                    continue
                dic = {}
                dic['first_code'] = node['first_code']
                dic['first_name'] = node['first_name']
                dic['second_code'] = item['second_code']
                dic['second_name'] = item['second_name']
                if self.is_update_mode:
                    current = self.start_date
                    while 1:
                        if current > self.end_date:
                            break
                        date = current.format('YYYY-MM-DD')
                        all_keys.append('%s=%s=%s' % (symbol, date, json.dumps(dic)))
                        current = current.shift(days=1)
                else:
                    # 代表report_date 为空，获取所有数据
                    all_keys.append('%s=%s=%s' % (symbol, '', json.dumps(dic)))
        return all_keys

    def yield_key(self):
        rv = None
        if self.__i < len(self.__all_keys):
            rv = self.__all_keys[self.__i]
            self.__i += 1
        return rv
