# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
import pymongo, pymysql, time
import pymysql.cursors
from scrapy import log
from twisted.enterprise import adbapi
class HousePricePipeline(object):
    def process_item(self, item, spider):
        if item["unit_price"] == 'no' or item["newest_release_ts"] == 'no':
            raise DropItem('bad data')
        else:
            return item

    

class MongoPipeline(object):
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE")
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        name = item.__class__.__name__
        if self._process_item(item, spider):
            file = item.pop("html_result")
            new_source_url = item["new_source_url"]
            self.db["RecordFiles"].insert({'html':[new_source_url,file],"crawltime": int(time.time())})
            self.db[name].insert(dict(item))
        else:
            return None
    def _process_item(self, item, spider):
        n = self.db["fingerprint"].find({"source_unique": item["source_unique"]})
        assert n.count() < 2
        if n.count() < 1:
            self.db["fingerprint"].insert(dict(new_price_id=item["new_price_id"],
                                                source_unique=item["source_unique"],
                                               create_ts=item["create_ts"],
                                               updated_ts=item["updated_ts"]))
            return True
        else:
            return False
#