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
    start_urls = [f"https://www.amazon.com/dp/{pid}" for pid in get_all_product_ids()[0:1]]


    # 存储原始HTML的目录
    HTML_STORE_DIR = "../data/crawled_html"
    os.makedirs(HTML_STORE_DIR, exist_ok=True)

    def parse(self, response):
        product_id = response.url.split("/dp/")[-1]
        item = AmazonMovieItem()
        item["product_id"] = product_id
        item["source_url"] = response.url

        # 1. 保存原始HTML到本地（用于血缘追踪）
        html_path = os.path.join(self.HTML_STORE_DIR, f"{product_id}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        item["html_path"] = html_path

        # 2. 解析电影元数据（Amazon页面结构可能变化，需根据实际调整XPath/CSS）
        # 电影名
        item["movie_name"] = response.css("#productTitle::text").get(default="").strip()

        # 上映时间（示例XPath，需调试）
        item["release_date"] = response.xpath('//span[contains(test(), "release date")]/following-sibling::span/text()').get(default="").strip()

        # 风格
        item["genre"] = ",".join([g.strip() for g in response.xpath('//th[contains(text(), "Genre")]/following-sibling::td/a/text()').getall()])

        # 导演
        item["director"] = response.xpath('//th[contains(text(), "Director")]/following-sibling::td/a/text()').get(default="").strip()

        # 全体演员
        item["cast"] = ",".join([c.strip() for c in response.xpath('//th[contains(text(), "Cast")]/following-sibling::td/a/text()').getall()])

        # 版本（如DVD/蓝光）
        item["version"] = response.xpath('//th[contains(text(), "Format")]/following-sibling::td/text()').get(default="").strip()

        yield item