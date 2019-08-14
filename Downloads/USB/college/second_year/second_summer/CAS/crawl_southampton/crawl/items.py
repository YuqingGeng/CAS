# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

# 图片以及文字文章
class Articleitem(scrapy.Item):
    table = "cra_technique_article"

    # 简介
    abstract = scrapy.Field()

    # 正文
    content = scrapy.Field()

    # 标题
    title = scrapy.Field()

    # url
    url = scrapy.Field()


    #搜索关键词
    search_keyword = scrapy.Field()


    @staticmethod
    def insert_command(item, cur_time, update_time, delete_time): #往数据库输入item内容以及time*3
        sql = 'INSERT into cra_technique_article(abstract, content, '\
             'create_at, delete_at, title, update_at, url) '\
              'VALUES ("{}", "{}", "{}", "{}", "{}", "{}", "{}")'\
            .format(item.get('abstract', None), item.get('content', None), 
                    cur_time, cur_time, item.get('title', None), cur_time, item.get('url', None))
        return sql

