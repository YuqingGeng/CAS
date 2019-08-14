# -*- coding: utf-8 -*-
import scrapy
import datetime
import re
import time
import random
import os
from PIL import Image
import traceback
from crawl.items import Articleitem
import urllib
from urllib import request 
from urllib.parse import quote 
from scrapy.utils.response import get_base_url

# selenium相关库
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

# scrapy 信号相关库
from scrapy.utils.project import get_project_settings
from scrapy import signals
from scrapy.linkextractors import LinkExtractor
from pydispatch import dispatcher
from scrapy.pipelines.files import FilesPipeline

# words=open (r'../data/keywords.txt', encoding='UTF-8')
words=open (r'../data/keywords.txt', encoding='UTF-8')

class southamptonSpider(scrapy.Spider):
    name = 'southampton'
    allowed_domains = ['southampton.ac.uk']
    start_urls=[]
    # 在图书馆界面搜索关键词
    all_url=[]

    for word in words:
        word=word.strip('\n')
        url='https://search.soton.ac.uk/Pages/projectsearch.aspx?k=' + str(urllib.parse.quote(word)) 
        all_url.append(url)
    start_urls=[url.strip() for url in all_url]

    


    def __init__(self):
        # 读取关键词列表中的关键词
        self.keywords = ''.join(words.readlines()).strip('\n').splitlines()
        self.header={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.3'
    }


    def parse(self, response):
        link_list=[]

        web_url=str(response).split(" ")[1][:-1]

        seleniumGoo=webdriver.Chrome()
        seleniumGoo.get(web_url)
        seleniumGoo.find_element_by_id('ctl00_m_g_6bd00495_e097_4a9b_b2fa_e3d8f4492e93_SF47197A8_go').click()

        #得到搜索关键词
        value=seleniumGoo.find_elements_by_xpath('//*[@id="ctl00_m_g_8961881c_a46e_40ab_bd30_488e92f46729_S760AEB6C_InputKeywords"]')[0].get_attribute("value")

        #link_list_page = seleniumGoo.find_element_by_xpath('//*[@id="SRB_g_7a7e5473_1075_4efa_bb14_b343224415ad_1_Title"]')
        link_list_elements = seleniumGoo.find_elements_by_xpath('//div[@class="srch-Title3"]/a')
        for element in link_list_elements:
            link_list.append(element.get_attribute("href"))
        for link in link_list:
            yield scrapy.Request(url=link, headers=self.header, callback=lambda response, values=value: self.content_parse(response, values))

        try:
            # 获取下一页
            next_page2=response.xpath('//a[@class="next-page-link"]/@href').extract()[0]
            next_page = response.urljoin(next_page2)
            yield scrapy.Request(url=next_page, headers=self.header, callback=self.parse)
        except:
            print("=========last page=========")

  


    def content_parse(self, response, values):
        item=Articleitem()

        #标题
        title = response.xpath("//*[@id='main-content']/div/h1/text()").extract()[0]
        item['title'] = title
        item['title'] = item['title'].replace('"', '”').replace("\n                    ", "")
        
        #正文
        content = response.xpath("//div[@class='uos-tier-inner']/p/text()").extract()
        content_new = []
        for con in content:
            con=con.replace('"', '\*').replace('\n                ', '').replace("'", '\*')
            content_new.append(con)
        item['content'] = content_new


        #摘要
        abstract=response.xpath("//*[@id='main-content']/div/p/text()").extract()[0]
        item['abstract'] = abstract
        item['abstract'] = item.get('abstract','').replace('"', '*').replace("'", '*')
        
        # url
        page_link=str(response).split(" ")[1][:-1]
        item['url'] = page_link

        #搜索关键词
        search_keyword = values
        item['search_keyword']=search_keyword
        return item
    
