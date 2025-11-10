import scrapy
import os
from amazon_movie_crawler.items import AmazonMovieItem

# 从原始数据集读取所有product_id（去重）
def get_all_product_ids():
    product_ids = set()
    with open("../data/products_id.txt", "r", encoding="utf-8") as f:
        for line in f:
            product_ids.add(line.strip())
    return list(product_ids)

class MovieSpider(scrapy.Spider):
    name = "amazon_movie"
    allowed_domains = ["amazon.com"]
    # start_urls = [f"https://www.amazon.com/dp/{pid}" for pid in get_all_product_ids()]
    start_urls = [f"https://www.amazon.com/dp/{pid}" for pid in get_all_product_ids()[0:0]]


    # 存储原始HTML的目录
    HTML_STORE_DIR = "../data/crawled_html"
    os.makedirs(HTML_STORE_DIR, exist_ok=True)

    def parse(self, response):
        # 如果不是电影(不包含标签imdbInfo_feature_div), 直接跳过
        if not response.xpath('//div[@id="imdbInfo_feature_div"]'):
            return

        product_id = response.url.split("/dp/")[-1]
        item = AmazonMovieItem()
        item["product_id"] = product_id
        item["source_url"] = response.url

        # 1. 保存原始HTML到本地（用于血缘追踪）
        html_path = os.path.join(self.HTML_STORE_DIR, f"{product_id}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        item["html_path"] = html_path

        # 2. 解析电影元数据
        # 电影名
        item["movie_name"] = response.css("#productTitle::text").get(default="").strip()

        # 上映时间
        item["release_date"] = response.xpath('//span[contains(text(), "Release date")]/following-sibling::span/text()').get(default="").strip()

        # 风格
        item["genre"] = response.xpath('//td/span[contains(text(), "Genre")]/ancestor::td/following-sibling::td/span/text()').get(default="").strip()

        # 导演
        item["director"] = response.xpath('//span[contains(text(), "Director")]/following-sibling::span/text()').get(default="").strip()

        # 全体演员(以","分隔)
        item["actors"] = response.xpath('//span[contains(text(), "Actors")]/following-sibling::span/text()').get(default="").strip()

        # 版本（如DVD/蓝光）
        item["version"] = ",".join(v for v in response.xpath('//div[@id="tmmSwatches"]/ul//a/span[@aria-label]/text()').getall())

        yield item