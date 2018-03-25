# -*- coding: utf-8 -*-
from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
import time, random, re, json, arrow
from uuid import uuid1
# from kafka import KafkaProducer
from apscheduler.schedulers.blocking import BlockingScheduler
import time, os
from house_price.items import HousePriceItem
from pyquery import PyQuery as pq
import multiprocessing
from datetime import datetime
import hashlib

sched = BlockingScheduler()

# kafka_host = '127.0.0.1'  # host
# kafka_port = 9092  # port
sumpat = re.compile(r'\d+')
# producer = KafkaProducer(bootstrap_servers=['{kafka_host}:{kafka_port}'.format(
#     kafka_host=kafka_host,
#     kafka_port=kafka_port
# )])
Noise = re.compile('\s+')
Date = re.compile('\d+-\d+-\d+')
relation_dict = {"用途：": "purpose", "物业地址：": "property_address", "建筑类型：": "building_type",
                 "绿化率：": "greening_rate", "容积率：": "volumetric_rate",
                 "占地面积：": "area_covered", "建筑面积：": "build_up_area",
                 "开发商：": "developers", "物业公司：": "property_company",
                 "土地使用年限：": "land_use_years", "开盘时间：": "opening_ts",
                 }


class CityhousePriceSpider(Spider):
    name = 'cityhouse_price'
    allowed_domains = ['cityhouse.cn']
    start_urls = ['http://www.cityhouse.cn/city.html']
    
    def parse(self, response):
        for url in response.xpath('//td[@class="right_city"]//span[@class="m_d_zx"]//a/@href').extract():
            urls = [url + '/ha/', url + '/forsale/']
            time.sleep(random.randrange(0, 4))
            yield Request(url=urls[0], callback=self.parse_new_turnaround, meta={'n': 1})
            yield Request(url=urls[1], callback=self.parse_old_turnaround, meta={'n': 1})
            
            # print(urls)
    
    def parse_new_turnaround(self, response):
        sum_page_str = response.xpath(
            '//div[@class="page1 mb5 clearfix"]//span[@class="page_p"]/text()').extract_first()
        n = response.meta['n']
        for url in response.xpath('//h4[@class="tit fl mr"]//a/@href').extract():
            url = response.urljoin(url)
            yield Request(url=url, callback=self.parse_page)
            if sum_page_str:
                sum_page = int(sumpat.findall(sum_page_str)[0])
                if n <= sum_page:
                    n += 1
                    after_url = response.url
                    if '/pg' not in after_url:
                        yield Request(url=response.url + "pg{}/".format(n), callback=self.parse_new_turnaround,
                                      meta={'n': n})
                    else:
                        yield Request(url=sumpat.sub(str(n), response.url), callback=self.parse_new_turnaround,
                                      meta={'n': n})
                        # response = producer.send('test', message_string.encode('utf-8'))
                        # print('new_url:',url)
    
    def parse_old_turnaround(self, response):
        sum_page_str = response.xpath(
            '//div[@class="page1 mb5 clearfix"]//span[@class="page_p"]/text()').extract_first()
        n = response.meta['n']
        # print(sum_page)
        for url in response.xpath('//h4[@class="tit"]//a/@href').extract():
            url = response.urljoin(url)
            yield Request(url=url, callback=self.parse_page)
            # print('old_url:', url)
        if sum_page_str:
            sum_page = int(sumpat.findall(sum_page_str)[0])
            if n <= sum_page:
                n += 1
                after_url = response.url
                # print(after_url)
                if '/pg' not in after_url:
                    yield Request(url=response.url + "pg{}/".format(n), callback=self.parse_old_turnaround,
                                  meta={'n': n})
                else:
                    yield Request(url=sumpat.sub(str(n), response.url), callback=self.parse_old_turnaround,
                                  meta={'n': n})
    
    def parse_page(self, response):
        # producer = KafkaProducer(bootstrap_servers=['{kafka_host}:{kafka_port}'.format(
        #     kafka_host=kafka_host,
        #     kafka_port=kafka_port
        # )])
        all_items = HousePriceItem()
        result_item = {
            "data_type": "CITY_HOUSE_PRICE",
            "rawdata": {response.url: response.text},
            "crawltime": int(time.time())
        }
        items = pq(response.text)
        sign = items('div[class="crumbs"] a:nth-of-type(2)').text()
        city = items('div[class="crumbs"] a:nth-of-type(1)').text().replace('房产', '')
        if '二手房' not in sign:
            all_items["new_price_id"] = str(uuid1())
            house_type = "new_house"
            comprehensive_score = items('h1[class="title"] span[class="red"]').text()
            if comprehensive_score:
                all_items["comprehensive_score"] = comprehensive_score
            else:
                all_items["comprehensive_score"] = 'no'
            l = items('span[class="f14 gray3"]')
            if l:
                l("script").remove()
                if l.text():
                    loc_item = l.text().split()
                    if len(loc_item) == 2:
                        district = loc_item[0]
                        street = loc_item[1]
                    elif len(loc_item) == 1:
                        district = loc_item[0]
                        street = 'no'
                    else:
                        district = 'no'
                        street = 'no'
                else:
                    district = 'no'
                    street = 'no'
            else:
                district = 'no'
                street = 'no'
            all_items["location"] = {"city": city, "district": district, "street": street}
            
            all_items["new_source_url"] = response.url
            all_items["house_type"] = house_type
            house_name = items('div[class="mt"] span[class="f16 b mr15"]').text()
            all_items["house_name"] = house_name
            classification_base = items('div[class="mt"] span[class="f14 gray9"]').text()
            if classification_base.split('：')[0] == '分类':
                classification = Noise.sub('', classification_base.split('：')[1].strip()).replace('|', ',')
            else:
                classification = 'no'
            all_items["classification"] = classification
            newest_release_time = items('ul[class="hs_layout2 mb"] span[class="time"]').text()
            if newest_release_time:
                newest_release_time = '-'.join(sumpat.findall(newest_release_time))
                newest_release_ts = arrow.get(newest_release_time).timestamp
            else:
                newest_release_ts = 'no'
            all_items["newest_release_ts"] = newest_release_ts
            last_news = items('div[class="city_detail_boxl"] div[class="cont clearfix"] li')
            if last_news:
                last_news = [{Date.findall(i.text())[0]: i.text()} for i in last_news.items()]
            all_items["last_news"] = last_news
            money_unit = items('ul[class="hs_layout2 mb"] li').text()
            unit_price = items('span[class="price_big"]').text()
            if unit_price:
                if '万元' in money_unit:
                    unit_price = float(unit_price) * 10000
                    all_items["unit_price"] = int(unit_price)
                else:
                    all_items["unit_price"] = int(unit_price.replace(',', '')) if ',' in unit_price else int(unit_price)
            else:
                all_items["unit_price"] = 'no'
            for item in items('div[class="hs_cont_infolist column2"] dl').items():
                item('a').remove()
                if item('dt').text() in list(relation_dict.keys()):
                    purpose = item('dd').text()
                    all_items[relation_dict[item('dt').text()]] = Noise.sub(r',', purpose)
            source_unique = ''.join([str(all_items["location"]), all_items["house_type"], all_items["house_name"],
                                     str(all_items["newest_release_ts"]), str(all_items["unit_price"])])
            source_unique = hashlib.md5(source_unique.encode()).hexdigest()
            all_items["source_unique"] = source_unique
            yield all_items
        
            
            
            
            
            # with open('/media/richard/0AD519050AD51905/my_files/shiyan/{}.json'.format(uuid.uuid1()), 'w') as f:
            #     f.write(json.dumps(result_item))
            # if 'forsale' in response.url:
            #     price = response.xpath('//span[@class="price"]//span[2]/text()').extract_first()
            # else:
            #     price = response.xpath('//span[@class="price"]//span[1]/text()').extract_first()
            #
            # city = response.xpath('//div[@class="crumbs"]//a[1]/text()').extract_first()
            # result = {
            #     'city':city,
            #     'price':price,
            #     'url':response.url
            # }
            # producer.send('test', json.dumps(result_item).encode('utf-8'))
            # yield result_item


# if __name__ == '__main__':
#
#     process = CrawlerProcess(
#         {
#             'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36'}
#     )
#     process.crawl(CityhousePriceSpider)
#     # process.start()
#     scheduler = BlockingScheduler()
#     scheduler.add_job(process.start(),'interval', seconds=25920)
#     scheduler.start()
#     print('press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'c'))
#     try:
#         while True:
#             scheduler.start()
#     except (KeyboardInterrupt, SystemExit):
#         scheduler.shutdown()
#         print('exit the job')
def my_job():
    process = CrawlerProcess(
        {
            'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36'}
    )
    process.crawl(CityhousePriceSpider)
    process.start()


# @sched.scheduled_job('interval', seconds=86400)
def run():
    process = multiprocessing.Process(target=my_job)
    process.start()
    process.join()


if __name__ == '__main__':
    sched.start()
