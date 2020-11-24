# coding: utf8
import os
import json
import arrow
from ...scheduled import Pipeline


class FileStorer(Pipeline):

    def __init__(self, *args, **kwargs):
        super(FileStorer, self).__init__(*args, **kwargs)
        self.data_dir = os.path.join(self.config['out_root_path'],
                                     self.config.get('tag', 'raw'))
        os.makedirs(self.data_dir, exist_ok=True)

    def process(self, key, data, options):
        if not data:
            return data, options

        symbol = options['symbol']
        for kind in ['cashflow', 'balance', 'income']:
            self.write_findata(symbol, kind, data[kind])

        return data, options

    def write_findata(self, symbol, kind, findata):
        mydir = os.path.join(self.data_dir, kind)
        os.makedirs(mydir, exist_ok=True)
        filename = os.path.join(mydir, '%s.csv' % symbol)
        text = '\n'.join([','.join(item) for item in findata])
        need_update = False
        if not os.path.exists(filename):
            need_update = True
        else:
            with open(filename) as f:
                old_text = f.read()

            if old_text.strip() != text:
                need_update = True
            else:
                self.logger.info('CheckSame:%s:%s' % (symbol, kind))


        meta_info = {}
        meta_file = filename + '.meta'

        assert((os.path.exists(filename) and os.path.exists(meta_file))
               or (not os.path.exists(filename) and not os.path.exists(meta_file)))

        if os.path.exists(meta_file):
            with open(meta_file) as f:
                meta_info = json.load(f)

        now = arrow.now().format('YYYY-MM-DD HH:mm:ss')
        if need_update:
            with open(filename, 'w') as f:
                f.write(text)

            meta_file = filename + '.meta'
            meta_info.update({'updated_at': now, 
                              'checked_at': now})
        else:
            meta_info.update({'checked_at': now})

        # write meta
        with open(meta_file, 'w') as f:
            f.write(json.dumps(meta_info))
