# -*- coding: utf-8 -*-

import pymysql
import time
import traceback
import json 
import codecs  

words=open (r'../crawl/data/keywords.txt', encoding='UTF-8')
keywords = ''.join(words.readlines()).strip('\n').splitlines()
words = keywords

#将生成的json加入cra_tech_article库
def insert_json_into_sql(item, cur_time, delete_time, update_time):
    sql = 'INSERT into cra_technique_article(abstract, content, '\
             'create_at, delete_at, title, update_at, url) '\
              'VALUES ("{}", "{}", "{}", "{}", "{}", "{}", "{}")'\
            .format(item.get('abstract', None), item.get('content', None), \
                    cur_time, delete_time, item.get('title', None), update_time, item.get('url', None))
    return sql

#对于每个新加入的文章，匹配tech_id与org_id，加入rel_org_tech表
def rel_tech_org():
    sql = 'SET sql_safe_updates = 0;'\
    'INSERT INTO cra_organization(name_en)'\
    'SELECT ("Southampton University") FROM dual'\
    'WHERE NOT EXISTS (SELECT*FROM cra_organization WHERE name_en="Southampton University");'\
    'SET @tech_id1=(SELECT @@Identity);'\
    '# SET @tech_id1=(select MAX(id) from cra_organization);'\
    'SET @org_id1=(select MAX(id) from cra_technique_article); '\
    'INSERT INTO rel_org_tech(tech_id, org_id, create_at) SELECT@tech_id1, @org_id1, sysdate() from dual'\
    'WHERE NOT EXISTS (SELECT * FROM rel_org_tech WHERE tech_id = @tech_id1 and org_id = @org_id1);'\
    'UPDATE cra_organization SET update_at = SYSDATE() WHERE name_en = "Southampton University";'\
    'UPDATE rel_org_tech SET update_at = SYSDATE() WHERE org_id =@org_id1;'
    return sql

#对于每个新加入的文章，匹配tech_id与keyword_id，加入rel_article_key表
def rel_tech_key(item):
    key=str(item["search_keyword"])
    print(key)
    title = item["title"]
    url = item["url"]
    sql = 'SET sql_safe_updates = 0; '\
    'SET @key_id1=(select id from technique_words WHERE keyword = "%s"); '\
    'SET @tech_id1=(select id from cra_technique_article WHERE url = "%s"); '\
    'INSERT INTO rel_article_keywords(article_id, keyword_id, create_at, type) SELECT@tech_id1, @key_id1, sysdate(), "1" from dual '\
    'WHERE NOT EXISTS (SELECT * FROM rel_article_keywords WHERE article_id = @tech_id1 and keyword_id = @key_id1); '\
    'UPDATE rel_article_keywords SET update_at = SYSDATE() WHERE article_id =@tech_id1; ' % (key, url)
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
        if len(result) > 0:
            return False 
        else:
            return True
        
    def insert_keywords_func(self, word, cur_time, update_time): 
        sql = 'INSERT into technique_words(keyword,create_at, update_at) '\
        'VALUES ("{}", "{}", "{}")'.format(word,cur_time,update_time) 
        'ELSE '\
        'UPDATE technique_words SET update_at = cur_time '
        return sql


    #根据相同abstract是否存在判断是否已经存在cra_tech库中
    def check_duplicate(self, item):
        sql='SELECT create_at FROM cra_technique_article WHERE (abstract = "{}")'.format(item['abstract'])
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
                sql = self.insert_keywords_func(i, self.cur_time, self.cur_time)
                print("@@@@@@@@")
                print(sql)
                try:
                    # 执行sql语句
                    self.cursor.execute(sql)
                    # 提交到数据库执行
                    self.connect.commit()
                    print("嘿嘿嘿")
                except Exception:
                    # Rollback in case there is any error
                    self.connect.rollback()
                    print("发生异常",Exception)
                    traceback.print_exc()
        print("==========finished=========")
        
    #将生成的json加入cra_tech_article库
    def json_to_sql(self):
        #data = []  
        file= open('../crawl/spiders/southampton.json','r', encoding  = 'utf-8')
        for line in file.readlines():  
            #str1=line.read()
            if line[-2]==",":
                line_new=json.loads(line[:-2]) #由于有 , 分割，需取倒数第二位
            else:
                line_new = json.loads(line[:-1])
#            print("======================")
#            print(type(line_new))
#            print("$$$$$$$$$$$$$$$$$$$$$")
                
                
                
            #检查是否已经在库内，如果是则打印“hey bro you're in DB already!”
            #不然，则插入新数据
            if self.check_duplicate(line_new):
#            
                sql = insert_json_into_sql(line_new, self.cur_time, self.cur_time, self.cur_time)
                self.cursor.execute(sql)
                self.connect.commit()
#                identity = do.getId()
#                
#                print("identity" , identity)
                sql2 = rel_tech_key(line_new)
                key=str(line_new["search_keyword"])
                tech_id= 'select id from technique_words WHERE keyword = "%s"; ' % (key)
                print("This is what sql will take in : "+ "\n" + tech_id)
                self.cursor.execute(tech_id)
                idd=self.cursor.fetchall()
                print("ID", idd[0][0])
                return 
                
            
#                sent = self.cursor.execute(sql[0])
#                self.connect.commit()
#
#                identity = self.cursor.execute(sql[1])
#                self.connect.commit()
#                print("sql: ", sent)
#                print ("tech_id : ", identity)
#                return 
#                print("sql2: ", sql2)
##                print("This is the tech_id : \n" , int(self.cursor.id))
#                return 
#
##                try:
#                    # 关联org_id与文章ID
#                self.cursor.execute(sql)
#                print("___________________________")
#                print(sql)
#                self.connect.commit()
#                #关联文章ID与关键词ID
#                print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
#                print(line_new["search_keyword"])
#                print(sql2)
#
#                self.cursor.execute(sql2)
#
#                self.connect.commit()
##                return
##                    try:
#                # data to rel_org_tech  
                sql3 = rel_tech_org() 
                print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
                print(sql3)
                return 
            
            
            
            
                self.cursor.execute(sql3)
                self.connect.commit()
#                        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
#                        rs = self.cursor.fetchall()
#                        for r in rs:
#                            print(r)
                print("okokokokokokokokokokokokokok")
                break
#                    except Exception:
#                        
##                        rs = self.cursor.fetchall()
##                        for r in rs:
##                            print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
##                            print(r)
#                        self.connect.rollback()
#                        print("nononononononononononononono", Exception)
#                except Exception:
##                    print("*************************")
##                    print(line_new)
##                    print("___________________________")
##                    self.connect.rollback()
#                    print("发生异常",Exception)
#                    traceback.print_exc()
                    
            else:
                print("hey bro you're in DB already!")
            return
        print ("success")

        
if __name__=='__main__':
#    SaveToMySQL().insert_keyword()
    SaveToMySQL().json_to_sql()