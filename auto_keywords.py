'''
1.根据关键词采集百度搜索结果页  可自定义采集个数
2.可保存到mysql数据库 或 本地文件夹
    a.保存本地 只保存关键词 + 标题 + 内容 (关键词在标题以  ## ##为记号)
    b.保存数据库 可以保存 标题  内容  原文链接 关键词  关键词的相关搜索词
        a.请正确配置数据库
        b.需要手动创建数据表 以及各个字段
3.需要定义一个数据清洗词表的 txt文件  里面可写正则表达式
4.需要手动创建 record.txt文件 用来保存已经抓取过的关键词
5.可以多线程  但未集成 代理
6.需要安装 bluextracter jieba pymysql lxml  requests redis等第三方包
7.手动设置 文章标题 与 关键词 相似度的判断分数 会影响到采集结果数量
8.手动添加 HEADERS 中的UA  以及COOKIES中的百度cookies
9.利用redis数据缓存进行url判断，已抓取过的url不再抓取
10.每篇文章提取最长的两个句子 求md5值来判断是否重复
使用问题请咨询 QQ：1129949173
'''
import requests
from bluextracter import Extractor
from threading import Thread
from queue import Queue
from lxml import etree
import re
import os
import random
import time
import jieba
import pymysql
import redis
from hashlib import md5

curdir = os.path.dirname(os.path.abspath(__file__))
os.chdir(curdir)

jieba.initialize()

HEADERS = [
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
    'Mozilla/5.0 (Windows;U;WindowsNT6.1;en-us)AppleWebKit/534.50(KHTML,likeGecko)Version/5.1Safari/534.50',
    'Mozilla/5.0 (Macintosh;IntelMacOSX10.6;rv:2.0.1)Gecko/20100101Firefox/4.0.1',
    'Mozilla/5.0 (Windows NT6.1;rv:2.0.1)Gecko/20100101Firefox/4.0.1',
    'Mozilla/5.0 (Macintosh;IntelMacOSX10_7_0)AppleWebKit/535.11(KHTML,likeGecko)Chrome/17.0.963.56Safari/535.11',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0',
    'Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18362',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 UBrowser/6.2.4098.3 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
    'Mozilla/5.0 (Macintosh;U;IntelMacOSX10_6_8;en-us)AppleWebKit/534.50(KHTML,likeGecko)Version/5.1Safari/534.50',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11) AppleWebKit/601.1.27 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/601.1.27',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36',
]

COOKIES = [
    r'BIDUPSID=B11DC34F571F93B83C40F78363D3577E; PSTM=1588473840; BAIDUID=B11DC34F571F93B8FE95481858135E16:FG=1; BD_UPN=12314353; BDUSS=WYtWn5LaEhSTWFlRUVvQ21OUjUyOHA3TFJvclV5eX5EN35HVWZGcS1BTmlvdGxlRVFBQUFBJCQAAAAAAAAAAAEAAAAuJLwU0uTT6tGp9q32rQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGIVsl5iFbJeR; H_WISE_SIDS=146748_143879_145945_146170_145498_147001_146538_145838_131247_144682_137746_144742_144250_146575_140259_127969_146548_146458_146750_147024_146732_146785_131423_146802_144658_142208_146001_146900_107314_146847_146136_139910_146824_140367_144966_145608_139883_146728_147204_146046_145397_143856_139914_110085; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; ispeed_lsm=2; SIGNIN_UC=70a2711cf1d3d9b1a82d2f87d633bd8a03413289766; delPer=0; BD_CK_SAM=1; PSINO=7; BD_HOME=1; sugstore=0; BDRCVFR[feWj1Vr5u3D]=I67x6TjHwwYf0; H_PS_PSSID=31619_1451_21113_31069_31660_31782_31321_30823_26350_22159; H_PS_645EC=554dqK8iNHLvBj7yfbW%2F8F0HvIJ6mwiPR3C1yh3NoVXfmKQxJm8nOlAGBxMJJQARLqjS; BDSVRTM=110',
    r'BAIDUID=16189268A7CCFB29696209538CC20C49:FG=1; PSTM=1562828745; BIDUPSID=6BD5CC05C058022DF6CB672F03EE98F5; NOJS=1; BDUSS=jAzblU5NDFPd2I4WWcxbFVFdmNpZ3FWallqNn5JelFXZzJoU0psd2djSFZLaTFlRVFBQUFBJCQAAAAAAAAAAAEAAAAuJLwU0uTT6tGp9q32rQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAANWdBV7VnQVeS; H_WISE_SIDS=141182_100806_137933_139405_128063_135846_141003_125695_139148_120197_138471_140853_141345_140334_138878_137985_140174_131247_132551_137745_118893_118858_118855_118818_118803_138165_138883_140260_141210_141368_140267_139057_140202_140592_136863_138585_139171_139626_140077_140113_136196_133847_140793_134256_131423_140311_136537_136752_110085_140325_127969_140954_140593_140864_139802_139886_139408_127416_138313_138426_141194_138942_140682_141191_140597_140962; BD_UPN=12314353; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; delPer=0; BD_CK_SAM=1; BD_HOME=1; SIGNIN_UC=70a2711cf1d3d9b1a82d2f87d633bd8a03355511966; uc_login_unique=57281200cd279fe37dccf1aaaf53a9bf; uc_recom_mark=cmVjb21tYXJrXzIxMTc5OTM2; BDRCVFR[SHOOb3ODEBt]=mk3SLVN4HKm; PSINO=6; H_PS_PSSID=1455_31117_21110_30840_31186_30903_30823_31085_26350_31196; COOKIE_SESSION=5_0_9_7_14_21_0_3_9_5_10_0_6062261_0_0_0_1585553495_0_1585553495%7C9%230_0_1585553495%7C1; BDRCVFR[feWj1Vr5u3D]=mk3SLVN4HKm; sugstore=0; H_PS_645EC=8c3edffvokmV8%2FS70gaxw3ocsQMKVbZPdigyICwpqEwt6tARKxpYQ2sa3tj0fpRtW91R',
    r'BAIDUID=A55D609BF6D2BFC9D55001A0665B012A:FG=1; BIDUPSID=A55D609BF6D2BFC9D55001A0665B012A; PSTM=1565409473; H_WISE_SIDS=130611_137735_132923_128699_136650_135964_136630_132549_134982_126062_136436_137157_120154_137001_133981_136365_132910_136455_137691_135846_131246_137743_132378_136680_118896_118873_118852_118820_118805_136687_107315_132783_136799_136430_136095_133351_137901_137221_136861_129656_136196_137104_133847_132552_137467_134046_129643_131423_137466_136740_110085_137863_127969_137625_136612_135416_128196_137695_136635_137096_137208_134383_136413_137450_136988; BDUSS=3RpVXhIbktSdGdmbUJjZnRiazJEVEppTjlPcHdUak1JYjFDT3ladH5xamZjZmxkRVFBQUFBJCQAAAAAAAAAAAEAAAB4NA4naGFpc2hlbmc5OQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAN~k0V3f5NFdZ; uc_login_unique=85d11417a5dfe306fc2a2f3b2eb9828d; uc_recom_mark=cmVjb21tYXJrXzI4MDAyOTQ5; SIGNIN_UC=70a2711cf1d3d9b1a82d2f87d633bd8a03357167400; BDRCVFR[tFA6N9pQGI3]=mk3SLVN4HKm; H_PS_PSSID=; delPer=0; PSINO=3; BDORZ=FFFB88E999055A3F8A630C64834BD6D0',
]

def get_pro():
    # 代理服务器
    proxyHost = "http-dyn.abuyun.com"
    proxyPort = "9020"

    # 代理隧道验证信息
    proxyUser = "HY9969190461286D"
    proxyPass = "CECA5103E867B917"

    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host" : proxyHost,
        "port" : proxyPort,
        "user" : proxyUser,
        "pass" : proxyPass,
    }

    proxies = {
        "http"  : proxyMeta,
        "https" : proxyMeta,
    }
    return proxies

# 建立本地redis数据库链接池  如果未设置连接密码  就去掉最后的password或设置为None
pool = redis.ConnectionPool(host='127.0.0.1', db=3, port=6379, decode_responses=True, password='root')

class BaiduRank(Thread):
    def __init__(self, kw_q, url_q, config, page):
        super().__init__()
        self.header = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'User-Agent': random.choice(HEADERS),
            'Cookie': random.choice(COOKIES),
            'Upgrade-Insecure-Requests': '1',
            'Host': 'www.baidu.com',
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh-TW;q=0.9,zh;q=0.8',
            'Cache-Control': 'max-age=0',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
        }
        self.page = page
        self.kw_q = kw_q
        self.url_q= url_q
        self.config = config
        self.record = open('record.txt', 'a+', encoding='utf-8')
        self.record_word = set(w.strip() for w in open('record.txt', 'r', encoding='utf-8').readlines())
        self.pool = self.config['pool']

    def run(self):
        while True:
            try:
                wd = self.kw_q.get()
                html = self.download(wd)
                if html is None:
                    print('关键词排名页面抓取失败')
                    continue
                self.extra_url(wd, html)
                if wd not in self.record_word:
                    self.record_word_save(wd) # 只保存抓取成功的关键词
            finally:
                self.kw_q.task_done()

    def download(self,wd,retries = 4,proxies=None):
        url = 'https://www.baidu.com/s?'
        paras = {
            'ie':'utf-8',
            'wd':wd,
            'rn':self.page,
            'tn':'baidu'
        }
        try:
            req = requests.get(url,params=paras,headers=self.header,timeout=20)
        except requests.RequestException as e:
            if retries > 0:
                time.sleep(1)
                print('百度页面重试抓取ing...')
                self.download(wd, retries - 1, proxies=get_pro())
            html = None
            # print('百度页面抓取出错',e)
            self.kw_q.put(wd)
        else:
            req.encoding = 'utf8'
            html = req.text
        return html

    def extra_url(self, wd, html, retries = 4):
        doc = etree.HTML(html)
        main = doc.xpath('//div[@id="content_left"]//div[@class="result c-container "]//h3//a/attribute::href')
        try:
            main1 = doc.xpath('//div[@id="rs"]//table//th//a/text()')
            likeword = ",".join(main1)
        except Exception:
            likeword = ""
        for item in main:
            try:
                req = requests.head(item)
            except requests.RequestException as e:
                if retries > 0:
                    time.sleep(1)
                    return self.extra_url(wd, html, retries-1)
                # print('链接解密失败', e)
                self.kw_q.put(wd)
            else:
                link = req.headers.get('Location')
                # print('解密后的链接',link)
                try:
                    if 'htm' in link  and '.baidu.com' not in link:
                        # print('真实链接为', link)
                        crawl_url = self.record_url_save(link)
                        if crawl_url:
                            self.url_q.put((wd, link, likeword))
                        else:
                            print("当前链接在redis中已有记录", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                            continue
                except:
                    continue
                else:
                    continue

    # 记录已抓取过的关键词
    def record_word_save(self, kw):
        self.record.write(f'{kw}\n')
        self.record.flush()
        self.record_word.add(kw)
    
    # 记录已抓取过的url
    def record_url_save(self, url):
        r = redis.Redis(connection_pool=self.pool)
        s = r.set(url,1,nx=True)
        return s


class ArtSpilder(Thread):
    def __init__(self, url_q, crawl_config, dbconfig):
        super().__init__()
        self.headers = {
            'User-Agent':'Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
            'Connection': 'keep-aliv',
        }
        self.url_q = url_q
        self.crawl_config = crawl_config
        self.conn = pymysql.connect(**dbconfig)
        self.stop_word = '|'.join(item.strip() for item in open(crawl_config["clean_word"], 'r', encoding='utf-8-sig').readlines())
        self.end_compile = re.compile(r'([,，。！!?？、\n~])', re.I)
        self.con_compile = re.compile(fr'({self.stop_word})', re.I)
        self.pool = self.crawl_config["pool"]

    def run(self):
        while True:
            try:
                wd, url, likeword = self.url_q.get()
                source = self.download(url)
                if source is None:
                    print("获取文章内容失败..")
                    del_res = self.del_redis_url(url)
                    if del_res:
                        print("已从redis删除未抓取成功的链接")
                    else:
                        print('从redis删除未成功抓取链接失败')
                    continue
                # print('正在抓取--->:',url)
                con_dict = self.extrac_art(url, source, wd, likeword)
                self._save_file(wd, con_dict)
            except:
                continue
            finally:
                self.url_q.task_done()
                

    def download(self,url,retries = 4, proxies=None):
        endco = ['utf8','utf-8','gbk','gb2312']
        try:
            req = requests.get(url,headers=self.headers, timeout=15, proxies=proxies)
            # s = requests.session()
            # s.keep_alive = False
        except requests.RequestException as e:
            if retries > 0:
                print("文章内容抓取重试ing....")
                time.sleep(1)
                return self.download(url, retries - 1, proxies=get_pro())
            # print('文章页抓取失败:',e)
            html = None
        else:
            html = req.text
            encod = self._extra_encod(html)
            encod = str(encod)
            if encod.lower() not in endco:
                encod = 'utf-8'
            req.encoding = encod
            html = req.text
        return html

    def extrac_art(self,url,html,wd, likeword=""):
        content_dict = {}
        ex = Extractor()
        ex.extract(url,html)
        title = ex.title
        title = title.replace("�", "")
        title = title.replace('/','')
        title = title.replace('\\','')
        title = title.replace('*','')
        title = title.replace('|','')
        title = title.replace('?','')
        title = re.sub(r'(/|-|"|_|\'|:|<|>)','',title)
        content = ex.format_text
        content = re.sub(r'http.*?\.html', '', content)
        score = ex.score
        if score > 10000 and '�' not in title:
            # 对文章进行简单的清洗
            # 1.切割文章为短句子
            con_body = self.end_compile.sub(r'\g<1>\n', content)
            con_arr = con_body.split('\n')
            clean_body = [] # 保存清洗之后的数据
            # 2.对切割之后的数据进行清洗
            for p in con_arr:
                if self.con_compile.search(p):
                    continue
                else:
                    clean_body.append(p)
            
            clean_content = "".join(clean_body) # 已经切割完成 且清洗完成的新内容
            longer_str = self.get_longer_sentence(clean_content)
            sign = md5(longer_str.encode("utf-8")).hexdigest()
            content_dict = {
                'title':title,
                'content':clean_content,
                'url': url,
                "word": wd,
                "likeword": likeword,
                "sign": sign
            }
        else:
            content_dict = {
                'title':'',
                'content':'',
                'url': url,
                "word": wd,
                "likeword": likeword,
                "sign": sign
            }
        return content_dict
    
    def _save_file(self, wd, con):
        if not isinstance(con, dict):
            return
        title = con['title']
        content = con['content']
        if self.count_simisc(wd, title) < self.crawl_config["limit_score"]:
            # print("相关度太低  不保存: ", title)
            return
        if len(content)< 300 or len(title) < 4 or '404错误' in title:
            # print("数据质量低 不保存")
            return
        if self.crawl_config["save_path"] == "mysql":
            try:
                with self.conn.cursor() as cursor:
                    sql = f"INSERT IGNORE INTO `{self.crawl_config['table_name']}` (`title`, `content`, `url`, `word`, `likeword`, `sign`) VALUES(%s, %s, %s, %s, %s, %s)"
                    res = cursor.execute(sql, args=(con["title"], con["content"], con["url"], con["word"], con["likeword"], con["sign"]))
            except Exception as e:
                print('数据库链接失败', e)
            else:
                if res:
                    print(f'--成功写入数据: {title}')
                else:
                    # print(f"链接重复 不保存{con['url']}")
                    pass
                return True
        else:
            # 保存位置
            floder_path = os.path.join(curdir, self.crawl_config['save_path'])
            folder = os.path.exists(floder_path)
            if not folder:
                os.mkdir(floder_path)
            title = f"##{con['word']}##{title}"
            try:
                print('标题：', title)
                with open(os.path.join(floder_path, f'{title}.txt'), 'w', encoding='utf-8') as f:
                    f.write(content)
            except OSError as e:
                print(f"本地保存失败: {title} ---", e)
                pass

    # 提取页面上的编码
    def _extra_encod(self,html):
        encod = re.findall(r'<meta.*?charset="?([\w-]*)".*>', html, re.I)
        encod = encod[0] if encod else "utf-8"
        return encod
    
    # 提取段落中最长的句子
    def get_longer_sentence(self, string):
        if not isinstance(string, str):
            return
        if string[0] in ',，。！？,：:、':
            string = string[1:]
        new_sentence = self.end_compile.sub(r'\g<1>\n', string)
        new_sentence = new_sentence.split('\n')
        longer_string = [s for s in new_sentence if len(s) > 30]
        if not longer_string:
            longer_string = new_sentence
        longer_string.sort(key= lambda x:len(x), reverse=True)
        longer_sentence = longer_string[0] + longer_string[1]
        return longer_sentence
    
    # 计算相关度
    @staticmethod
    def count_simisc(word, title):
        wd_set = set(jieba.lcut_for_search(word))
        tt_set = set(jieba.lcut_for_search(title))
        simisc = wd_set - tt_set
        try:
            score = 1 - len(simisc) / len(wd_set)
        except:
            score = 1.0
        return score
    
    # 如果文章url未抓取成功 就从redis中删除记录
    def del_redis_url(self,url):
        if not isinstance(url, str):
            return False
        r = redis.Redis(connection_pool=self.pool)
        s = r.delete(url)
        return s

def go_spider(config, dbconfig):
    kw_q = Queue()
    url_q = Queue()
    num = config["page"]
    # 待采集关键词位置
    txt_path = os.path.join(curdir, config["target_file"])
    arr = [line.strip() for line in open( txt_path ,'r', encoding='utf-8').readlines()] #待采集关键词列表
    record_arr = [line.strip() for line in open(config["record"] ,'r', encoding='utf-8').readlines()] # 已经抓取过的关键词
    difrence_arr = list(set(arr).difference(set(record_arr))) # 求差集  只拿未成功采集和未采集的词
    print("待采集关键词个数 " , len(difrence_arr))
    for wd in difrence_arr:
        wd = re.sub(r'\s', '', wd)
        if len(wd) >= 4:
            kw_q.put(wd)
        else:
            continue

    for i in range(50):
        br = BaiduRank(kw_q, url_q, config, num)
        br.setDaemon(True)
        br.start()

    for j in range(100):
        art = ArtSpilder(url_q, config, dbconfig)
        art.setDaemon(True)
        art.start()

    kw_q.join()
    url_q.join()

if __name__ == "__main__":
    # 数据库配置变量
    dbconfig = dict(
        host= '127.0.0.1',
        port= 3306,
        user= 'root',  # msyql用户名
        password= 'root',  # MySQL密码
        db= 'game',  # 数据库名称
        autocommit= True,
        charset= 'utf8mb4'  # 数据库的字符集编码
    )
    # 抓取爬虫相关配置变量  
    crawl_config = dict(
        pool=pool,
        target_file= "words.txt",  # 要抓取的关键词文件名
        dbconfig= dbconfig,
        table_name= "fgo",  # 数据保存到哪个表里面
        save_path= "mysql",  # 如果填写的值是 mysql，那么数据就会保存到MySQL数据库，如果是其它名称，那么就会保存到本地指定的文件夹
        record= "record.txt", # 已采集过的关键词保存到哪个文件
        page= 30, # 采集百度排名前多少个
        clean_word= "clean.txt", # 文章清洗词表  可以写正则表达式
        limit_score= 0.4 # 定义文章与关键词相似度判断的最低分数
    )
    start_time = time.time()
    go_spider(crawl_config, dbconfig)
    print(f'用时{time.time() - start_time}')
    print('执行完毕.....')
