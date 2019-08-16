# -*- coding: utf-8 -*-
'''
author: Yuqing Geng
date: Aug. 14th 2019

=========
说明：这个文档是用于将json文件入库：
本篇用到的json文件是由spiders文件夹中的 mit.py文件生成，用到了scrapy爬虫，
使用scrapy crawl mit 可重新生成json
=========

=========
详细说明：
=========
其中json插入 cra_paper, 取 paper_id, 设置type = 1
搜索这篇文章的关键词插入 technique_words ，取key_id，
文章作者插入 cra_author ,取 author_id,
这篇文章所属机构插入cra_org， 取org_id,
-----------------
然后将文章--key的对应(paper_id, key_id)rel_article_keyword
将文章--机构的对应(paper_id, org_id)插入rel_org_tech。
将文章--作者的对应(paper_id, author_id)插入rel_author_paper
将作者--机构对应(paper_id, org_id)插入rel_author_org
-------------------

注：本篇用到的是本地localhost库，如果为了入库请注释掉67行，同时解开68行的注释。
注：逻辑设定为若cra_technique_article中已有现有的文章，默认完成了关联表，直接弹出“what's up  bro‘代表已入库

'''
import pymysql
import time
import traceback
import json 

###sql 中replace
#use knowledgeproject;
#SET sql_safe_updates = 0; 
#update cra_technique_article set content = replace (content, '"', '“');


words=open (r'./crawl/data/keywords.txt', encoding='UTF-8')
keywords = ''.join(words.readlines()).strip('\n').splitlines()
words = keywords

# 插入org
def insert_org(org_name, time):
    sql = 'INSERT INTO cra_organization(name_en) '\
    'SELECT "%s" FROM dual '\
    'WHERE NOT EXISTS (SELECT*FROM cra_organization WHERE name_en= "%s"); ' %(org_name, org_name)
    return sql 

#将生成的json加入 cra_paper 库
def insert_json_into_sql(item, update_time):
    sql = 'INSERT into cra_paper(title_en, keywords_en, '\
             'abstract_en, update_at, authors_affi_en, '\
             'authors_en, isbn, journal_conf_en, paper_year, pdf, paper_page_url) '\
              'VALUES (\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\")'\
            .format(item.get('title_en', None), item.get('search_keyword', None), \
                item.get('abstract_en', None), update_time, item.get('authors_affi_en', None), \
                item.get('authors_en', None), item.get('isbn', None), item.get('journal_conf_en', None), \
                item.get('paper_year', None), item.get('pdf', None), item.get('paper_page_url', None))
    return sql

class SaveToMySQL(object):
    def __init__(self):
        # 连接mysql数据库
        self.connect = pymysql.connect(host='localhost', user='root', password='931009Gyq@', db='knowledgeproject', port=3306)
        #self.connect = pymysql.connect(host='rm-bp151dcc75aycqd80to.mysql.rds.aliyuncs.com', user='root', password='Iscas123', db='knowledgeproject', port=3306)
        self.cursor = self.connect.cursor()
        self.cur_time = time.strftime("%Y-%m-%d %H:%M:%S")
        
    #把搜索用的关键词加入sql的tech_words库
    def check_keywords(self, word):
        sql='SELECT create_at FROM technique_words WHERE (keyword = "{}")'.format(word)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        #如果已存在则不入库
        if len(result) > 0:
            return False 
        else:
            return True
        
    def insert_keywords_func(self, word): 
        sql = 'INSERT into technique_words(keyword, update_at) '\
        'VALUES ("{}", "{}")'.format(word,self.cur_time) 
        'ELSE '\
        'UPDATE technique_words SET update_at = "%s" ' % (self.cur_time)
        return sql
    
        
    def insert_author(self, author):
        sql = 'INSERT into cra_author(name, update_at) '\
        'SELECT "%s", "%s" from dual '\
        'WHERE NOT EXISTS (SELECT * FROM cra_author WHERE name = "%s"); '\
        % (author , self.cur_time, author)
        return sql 

    #根据相同abstract是否存在判断是否已经存在cra_tech库中
    def check_duplicate(self, item):
        sql='SELECT create_at FROM cra_paper WHERE (abstract_en = "{}")'.format(item['abstract_en'])
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        if len(result) > 0:
            return False
        else:
            return True
        
    #把搜索用的关键词加入sql的tech_words库
    def insert_keyword(self):
        for i in words:
            if self.check_keywords(i):
                sql = self.insert_keywords_func(i)
                print("inserted keywords")
                try:
                    # 执行sql语句
                    self.cursor.execute(sql)
                    # 提交到数据库执行
                    self.connect.commit()
                    print(" insert keywords")
                except Exception:
                    # Rollback in case there is any error
                    self.connect.rollback()
                    print("发生异常",Exception)
                    traceback.print_exc()
        print("==========successfully insert keywords=========")
        
    def rel_paper_key(self, key_id, tech_id):
        sql='INSERT INTO rel_article_keywords(article_id, keyword_id, update_at, type) SELECT %s, %s, "%s", 1 from dual '\
    'WHERE NOT EXISTS (SELECT * FROM rel_article_keywords WHERE article_id = %s and keyword_id = %s); '\
    % (tech_id, key_id, self.cur_time, tech_id, key_id) 
        return sql 
    
    def rel_paper_org(self, tech_id, org_id):
        sql = 'INSERT INTO rel_org_tech(org_id, tech_id, update_at) SELECT %s, %s, "%s" from dual '\
        'WHERE NOT EXISTS (SELECT * FROM rel_org_tech WHERE org_id = %s and tech_id = %s); '\
        %(org_id, tech_id, self.cur_time, org_id, tech_id)
        return sql 
    
    def rel_author_paper(self, paper_id, author_id):
        sql = 'INSERT INTO rel_author_paper(author_id, paper_id, update_at) '\
        'SELECT %s, %s, "%s" from dual '\
        'WHERE NOT EXISTS (SELECT * FROM rel_author_paper WHERE paper_id = %s and author_id = %s); '\
        %(author_id, paper_id, self.cur_time, paper_id, author_id)
        return sql 
    
    def rel_author_org(self, author_id, org_id):
        sql = 'INSERT INTO rel_author_org(author_id, org_id, update_at) '\
        'SELECT %s, %s, "%s" from dual '\
        'WHERE NOT EXISTS (SELECT * FROM rel_author_org WHERE author_id = %s and org_id = %s); '\
        %(author_id, org_id, self.cur_time, author_id, org_id)
        return sql 
    
    ## 插入org
    def insert_org_application(self, org):
        sql = insert_org(org, self.cur_time)
        self.cursor.execute(sql)
        self.connect.commit()
#        print("========= inserted org =============")
        return 
    
    #将生成的json加入cra_tech_article库
    def json_to_sql(self):
        file= open('./crawl/spiders/mit.json','r', encoding  = 'utf-8')
        # step 1: insert organization into cra_org 

        for line in file.readlines():  
            if line[-2]==",":
                line_new=json.loads(line[:-2]) #由于有 , 分割，需取倒数第二位
            else:
                line_new = json.loads(line[:-1])

                
                
            #检查是否已经在库内，如果是则打印“hey bro you're in DB already!”
            #不然，则插入新数据
            if self.check_duplicate(line_new):
                ## json插入cra_tech_art库
                sql = insert_json_into_sql(line_new, self.cur_time)
#                print("insert cra_paper")
                try:
                    self.cursor.execute(sql)
                    self.connect.commit()
                except:
                    print("============insert cra_paper error====================")
                    print(sql)

                ## 建立key_id, paper_id,org_id,author_id variable 
                key=str(line_new["search_keyword"])
                paper = str (line_new['title_en'])
                org = str(line_new['authors_affi_en'])
                author = str(line_new['authors_en'])
                
                ## 插入org
                self.insert_org_application(org)
#                print("insert org")
                
                ## 插入author 
                sql = self.insert_author(author)
                self.cursor.execute(sql)
                self.connect.commit()
                
                ## 获取 key_id， author_id, paper_id, org_id
                key= 'select id from technique_words WHERE keyword = "%s"; ' % (key)
                paper = 'select id from cra_paper where title_en = "%s"; ' % (paper)
                org = 'select id from cra_organization where name_en = "%s"; '% (org)
                author = 'select id from cra_author where name = "%s"; ' % (author)
                
                self.cursor.execute(key)
                key_id=self.cursor.fetchall()[0][0]
                self.cursor.execute(author)
                author_id = self.cursor.fetchall()[0][0]
                self.cursor.execute(paper)
                paper_id = self.cursor.fetchall()[0][0]
                self.cursor.execute(org)
                org_id= self.cursor.fetchall()[0][0]

                ## 将 paper_id, key_id插入rel_articel_key关联表
                sql = self.rel_paper_key(key_id, paper_id)
                try:
                    self.cursor.execute(sql)
                    self.connect.commit()
#                    print("success rel_article_key")
                except:
                    print("=====NO rel_paper_key==========")
                    print(sql)
                
                ## 将org_id, paper_id 插入rel_org_tech关联表
                sql = self.rel_paper_org(paper_id, org_id)
                try:
                    self.cursor.execute(sql)
                    self.connect.commit()
#                    print("success rel_org_tech!")
                except:
                    print("======NO rel_org_Tech========")
                    print(sql)
                
                ## 将paper_id, author_id插入rel_author_paper关联表
                sql = self.rel_author_paper(paper_id, author_id)
                try:
                    self.cursor.execute(sql)
                    self.connect.commit()
#                    print("success rel_author_paper")
                except:
                    print("NO rel_author_paper")
                    print(sql)
                
                ## 将paper_id, org_id 插入rel_author_org 关联表
                sql = self.rel_author_org(author_id, org_id)
                try:
                    self.cursor.execute(sql)
                    self.connect.commit()
#                    print("success rel_author_org")
                except:
                    print("======NO rel_author_org=========")
                    print(sql)
            else:
                print("what's up bro")                
            
        print ("success")

        
if __name__=='__main__':
    SaveToMySQL().insert_keyword()
    SaveToMySQL().json_to_sql()