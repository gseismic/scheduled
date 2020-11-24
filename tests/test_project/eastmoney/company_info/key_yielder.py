# coding: utf8
import scheduled
from finapi.eastmoney import EmListAPI


class KeyYielder(scheduled.KeyYielder):

    def __init__(self, *args, **kwargs):
        super(KeyYielder, self).__init__(*args, **kwargs)
        self.api = EmListAPI(logger=self.logger)

    def yield_key(self):
        stocks = self.api.get_hs_stocks()
        for stock in stocks:
            yield '%s%s' % (stock['market'], stock['code'])
