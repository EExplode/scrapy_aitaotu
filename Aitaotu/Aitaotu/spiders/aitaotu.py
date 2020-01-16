# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from Aitaotu.items import AitaotuItem


class AitaotuSpider(scrapy.Spider):
    name = 'aitaotu'
    allowed_domains = ['aitaotu.com']
    start_urls = ['https://www.aitaotu.com/guonei/']

    def parse(self, response):
        urls = response.xpath('//div[@id="infinite_scroll"]//div[@class="img"]/a/@href').extract()
        for url in urls:
            url = response.urljoin(url)
            yield Request(url, callback=self.get_image)

        if response.xpath('//div[@id="pageNum"]/a[last()-1]/@href').extract():
            next_page = response.xpath('//div[@id="pageNum"]/a[last()-1]/@href').extract()[0]
            next_page = response.urljoin(next_page)
            yield Request(next_page, callback=self.parse)

    def get_image(self, response):
        item = AitaotuItem()
        item['image'] = response.xpath('//div[@class="big-pic"]//img/@src').extract()[0]
        item['referer'] = response.url
        item['image_name'] = ''.join(response.xpath('//div[@id="photos"]/h1//text()').extract()).strip()
        yield item

        if response.xpath('//div[@class="pages"]/ul/li[last()-1]/a/@href').extract():
            next_image = response.xpath('//div[@class="pages"]/ul/li[last()-1]/a/@href').extract()[0]
            next_image = response.urljoin(next_image)
            yield Request(next_image, callback=self.get_image)
