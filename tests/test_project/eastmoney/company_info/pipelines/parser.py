from scheduled import Pipeline
import arrow


class Parser(Pipeline):

    def process(self, key, data, options):
        '''
        {"data": {"jbzl": {"gsmc": "深圳市皇庭国际企业股份有限公司", "ywmc": "Shenzhen Wongtee International Enterprise Co., Ltd.", 
        "cym": "深国商->G深国商->深国商->*ST国商->深国商", "agdm": "000056", "agjc": "皇庭国际", 
        "bgdm": "200056", "bgjc": "皇庭B", "hgdm": "--", "hgjc": "--", "zqlb": "深交所主板A股", 
        "sshy": "房地产", "ssjys": "深圳证券交易所", "sszjhhy": "租赁和商务服务业-商务服务业", 
        "zjl": "陈小海", "frdb": "郑康豪", "dm": "曹剑", "dsz": "郑康豪", "zqswdb": "马晨笛", 
        "dlds": "王培,孙俊英,陈建华", "lxdh": "0755-82281888", "dzxx": "htgj@wongtee000056.com", 
        "cz": "0755-82566573", "gswz": "www.wongtee000056.com", 
        "bgdz": "广东省深圳市福田区福华路350号岗厦皇庭大厦(即皇庭中心)28楼", 
        "zcdz": "深圳市福田区福田街道岗厦社区福华路350号岗厦皇庭大厦28A01单元", "qy": "广东", "yzbm": "518048", 
        "zczb": "11.7亿", "gsdj": "914403001921790834", "gyrs": "618", "glryrs": "14", "lssws": "北京市中伦(深圳)律师事务所", 
        "kjssws": "立信会计师事务所(特殊普通合伙)", 
        "gsjj": "    深圳市皇庭国际企业股份有限公司是一家集不动产运营管理、金融服务、内容服务为一体的集团化上市公司,市值规模过百亿。公司创建于1983年,由经营零售商业起步。1993年改组为股份公司,成为深圳本地一家零售商业类A+B股上市公司,随后公司进入地产开发经营领域,先后开发出国企大厦、港逸豪庭等系列优质楼盘,2013年公司首个商业地产开发并运营的大型高端购物中心项目,皇庭广场正式开业。2015年起,公司深化战略转型,制定了“以轻资产模式为导向,构建以提供商业不动产运营为基础的运营服务平台、以提供资产管理为核心的金融服务平台、以提供儿童与青少年主题为特色的内容服务平台的三大业务平台”的战略,致力于将公司打造成为“中国领先的不动产综合解决方案提供商,不动产综合服务领域的领导品牌”。", 
        "jyfw": "经销日用百货、文化用品、纺织品、服装、劳保用品、家用电子产品、交通器材、五金工具、家用电器、日用美术陶瓷、家具、糖果糕点、饮料、干鲜果品、进出口业务按深贸管审证字第012号外贸企业审定证书办理(凡属专营商品按规定办)、土产品、装饰材料、糖、工艺美术品、副食品、五金杂品。"}, 
        "fxxg": {"clrq": "1985-01-19", "ssrq": "1996-07-08", "fxsyl": "--", "wsfxrq": "1996-06-21", "fxfs": "网上定价发行", "mgmz": "1.00", "fxl": "2000万", "mgfxj": "4.78", "fxfy": "--", "fxzsz": "9560万", "mjzjje": "9110万", "srkpj": "9.00", "srspj": "8.65", "srhsl": "61.42%", "srzgj": "9.68", "wxpszql": "--", "djzql": "1.16%"}, 
        "Code": "SZ000056", "CodeType": "ABStock", "SecuCode": "000056.SZ", 
        "SecurityCode": "000056", "SecurityShortName": "皇庭国际", "MarketCode": "02", "Market": "SZ", "SecurityType": null, "ExpireTime": "/Date(-62135596800000)/"}, "key": "SZ000056", "created_at": "2020-10-27 16:50:58"}
        '''
        if not data:
            return data, options

        now = arrow.now()
        strnow = now.format('YYYY-MM-DD HH:mm:ss')

        common = {
            'symbol': options['symbol'].upper(),
            'code': data['SecurityCode'],
            'name': data['SecurityShortName']
        }

        if (data['Market'] + data['SecurityCode'] != options['symbol']
            or data['SecurityCode'] != data['SecuCode'].split('.')[0]):
            print('='*50)
            print('data', data)
            print('options', options)
            raise Exception('Key-Result Inconsistence')
            # raise KeyboardInterrupt

        basic_info = {}
        basic_info.update(common)
        basic_info.update({'market': data['Market']})
        basic_info.update({'MarketCode': data['MarketCode']})
        basic_info.update({'updated_at': strnow})
        basic_info.update(data['jbzl'])

        ipo_info = {}
        ipo_info.update(common)
        ipo_info.update({'updated_at': strnow})
        ipo_info.update(data['fxxg'])

        options['parser'] = dict(basic_info=basic_info, ipo_info=ipo_info)
        return data, options
