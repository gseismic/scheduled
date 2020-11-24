### ChangeLog
应用场景: 
    - 本地多线程数据处理: key=filename 处理hexun-minute/sina-day-kline数据入库 finio
    - 获取并处理固定列表的网络数据: key=symbol+date，可线程多开，断点继续处理，并行处理
    - 通过数据计算指标

不应用的场景:
    - sina实时盘口

### Known Issues
    2020-03-13 19:55:45 storage中只有redis_.py 经过测试的，其他的接口不完整

fetcher     : fetch data from the Internet
scheduler   : 调度运行
generator   : 生产Key
producer    : 生产Key

### ChangeLog
    2020-03-25 11:29:04 process(self, data, key) -> process(self, key, data, option)
    2020-09-25 14:20:27 debug -> info, info -> notice
