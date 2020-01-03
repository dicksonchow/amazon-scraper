# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo


class AmazonScraperMongoPipeline(object):

    def __init__(self):
        connection = pymongo.MongoClient(
            'localhost',
            27017
        )
        db = connection['amazon']
        self.collection = db['flower_dress']

    def process_item(self, item, spider):
        # findOrCreate implemented with the update method with upsert = true
        self.collection.update({'title': item['title']}, dict(item), True)
        return item
