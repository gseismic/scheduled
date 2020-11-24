# coding: utf8
import arrow
from .. import scheduled
from finapi.eastmoney import EmListAPI


class OnlineKeyYielder(scheduled.KeyYielder):

    def __init__(self, config=None, logger=None):
        super(OnlineKeyYielder, self).__init__(config, logger)
        self.api = EmListAPI(logger=self.logger)
        self.__all_keys = self.get_all_keys()
        self.__i = 0

    def get_all_keys(self):
        symbols = self._get_all_symbols()
        return symbols

    def yield_key(self):
        rv = None
        if self.__i < len(self.__all_keys):
            rv = self.__all_keys[self.__i]
            self.__i += 1
        return rv

    def _get_all_symbols(self):
        # 包含A股和B股, 指数
        stocks = self.api.get_hs_stocks()
        s_symbols = ['S:' + i['market'] + i['code'] for i in stocks]
        # time.sleep(1)
        # indices = self.api.get_hs_indices()
        # i_symbols = ['I:' + i['market'] + i['code'] for i in indices]
        # return i_symbols + s_symbols
        return s_symbols
