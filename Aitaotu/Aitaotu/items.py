# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AitaotuItem(scrapy.Item):
    collection = table = 'images'

    # 图片链接
    image = scrapy.Field()
    # 图片链接请求头关键参数，不然下载出现404
    referer = scrapy.Field()
    # 图片名称
    image_name = scrapy.Field()
