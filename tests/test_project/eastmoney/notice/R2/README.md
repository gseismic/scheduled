## 定位
    netease 日K
    可以用于更新每日数据，或修复历史数据

##用法
    if days_to_today:
        start = now.shift(days=-abs(days_to_today))
        self.start_date = start.format('YYYY-MM-DD')
        self.end_date = now.format('YYYY-MM-DD')
    else:
        self.start_date = self.config.get('start_date', '1990-01-01')
        self.end_date = self.config.get('end_date')
        if self.end_date is None:
            self.end_date = now.format('YYYY-MM-DD')

##Database Model
    db.execute('PRAGMA synchronous = NORMAL')
    db.execute('PRAGMA journal_mode = WAL')

## 模板变化
    self.db_tagname = self.config.get('db_tagname', 'dayprice_netease')

## ChangeLog Tags
    2020-08-07 11:03:05
    2020-08-09 14:16:23
