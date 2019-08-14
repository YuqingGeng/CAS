# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import os
import scrapy
from crawl.settings import FILES_STORE as files_store
from scrapy.pipelines.files import FilesPipeline
import pymysql
import time
import traceback

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)

# 将文字信息存入到.json文件中
class JsonWriterPipeline(object):

    def open_spider(self, spider):
        self.file = open('southampton.json', 'w', encoding = 'utf-8')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(dict(item), ensure_ascii = False, cls=MyEncoder) + ",\n"
        self.file.write(line)
        return item


# 将信息存入到mysql数据库中
class SaveToMySQL(object):
    def __init__(self):
        # 连接mysql数据库
        self.connect = pymysql.connect(host='localhost', user='root', password='931009Gyq@', db='knowledgeproject', port=3306)
        #self.connect = pymysql.connect(host='rm-bp151dcc75aycqd80to.mysql.rds.aliyuncs.com', user='root', password='Iscas123', db='knowledgeproject', port=3306)
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):

        cur_time = time.strftime("%Y-%m-%d %H:%M:%S")
        # 判断有没有重复的数据
        sql = 'SELECT create_at FROM {} ' \
              'WHERE (title = "{}")'.format(item.table, item['title'])
        
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()

            # 如果有重复的标题则跳过不写
            if len(results) > 0:
                pass
            # 没有重复的情况下
            else:
                sql = item.insert_command(item, cur_time, cur_time, cur_time)
                try:
                    # 执行sql语句
                    self.cursor.execute(sql)
                    # 提交到数据库执行
                    self.connect.commit()

                except Exception:
                    # Rollback in case there is any error
                    self.connect.rollback()
                    print("发生异常",Exception)
                    traceback.print_exc()
                print("==========finished=========")
                print(sql)
        except Exception:
            print("Error: unable to fecth data")




        
        return item

    def close_spider(self, spider):
        self.connect.close()




