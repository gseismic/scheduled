import os
import sqlite_utils
from scheduled import Pipeline


class SqliteStorer(Pipeline):

    def __init__(self, *args, **kwargs):
        Pipeline.__init__(self, *args, **kwargs)
        self.out_root_path = self.config['out_root_path']
        os.makedirs(self.out_root_path, exist_ok=True)

    def process(self, key, data, options):
        if not data:
            return data, options

        dbfile = os.path.join(self.out_root_path, 'company_info.sqlite3')
        db = sqlite_utils.Database(dbfile)
        db.enable_wal()

        d_parser = options['parser']
        db['basic_info'].insert(d_parser['basic_info'], replace=True, pk='symbol')
        db['ipo_info'].insert(d_parser['ipo_info'], replace=True, pk='symbol')

        return data, options
