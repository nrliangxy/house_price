# -*- coding: utf-8 -*-
from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
import time, random, re, json, uuid
from kafka import KafkaProducer
from apscheduler.schedulers.blocking import BlockingScheduler
import time, os
import multiprocessing
from datetime import datetime
sched = BlockingScheduler()

kafka_host = '127.0.0.1'  # host
kafka_port = 9092  # port
sumpat = re.compile(r'\d+')
# producer = KafkaProducer(bootstrap_servers=['{kafka_host}:{kafka_port}'.format(
#     kafka_host=kafka_host,
#     kafka_port=kafka_port
# )])


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
        producer = KafkaProducer(bootstrap_servers=['{kafka_host}:{kafka_port}'.format(
            kafka_host=kafka_host,
            kafka_port=kafka_port
        )])
        result_item = {
            "data_type": "CITY_HOUSE_PRICE",
            "rawdata": {response.url: response.text},
            "crawltime": int(time.time())
        }
        with open('/media/richard/0AD519050AD51905/my_files/shiyan/{}.json'.format(uuid.uuid1()), 'w') as f:
            f.write(json.dumps(result_item))
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
        producer.send('test', json.dumps(result_item).encode('utf-8'))
        yield result_item


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
    
@sched.scheduled_job('interval', seconds=86400)
def run():
    process = multiprocessing.Process(target=my_job)
    process.start()
    process.join()

if __name__ == '__main__':
    sched.start()
