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
    start_urls = [f"https://www.amazon.com/dp/{pid}" for pid in get_all_product_ids()]
    # start_urls = [f"https://www.amazon.com/dp/{pid}" for pid in get_all_product_ids()[0:1]]


    # 存储原始HTML的目录
    HTML_STORE_DIR = "../data/crawled_html"
    os.makedirs(HTML_STORE_DIR, exist_ok=True)

    def parse(self, response):
        # 如果不是电影(不包含标签imdbInfo_feature_div), 直接跳过
        if not response.xpath('//div[@id="imdbInfo_feature_div"]'):
            return

        product_id = response.url.split("/dp/")[-1]
        # item = AmazonMovieItem()
        # item["product_id"] = product_id
        # item["source_url"] = response.url

        # 1. 保存原始HTML到本地（用于血缘追踪）
        html_path = os.path.join(self.HTML_STORE_DIR, f"{product_id}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        # item["html_path"] = html_path

        # # 2. 解析电影元数据
        # # 电影名
        # item["movie_name"] = response.css("#productTitle::text").get(default="").strip()

        # # 上映时间
        # item["release_date"] = response.xpath('//span[contains(text(), "Release date")]/following-sibling::span/text()').get(default="").strip()

        # # 风格
        # item["genre"] = response.xpath('//td/span[contains(text(), "Genre")]/ancestor::td/following-sibling::td/span/text()').get(default="").strip()

        # # 导演
        # item["director"] = response.xpath('//span[contains(text(), "Director")]/following-sibling::span/text()').get(default="").strip()

        # # 全体演员(以","分隔)
        # item["actors"] = response.xpath('//span[contains(text(), "Actors")]/following-sibling::span/text()').get(default="").strip()

        # # 版本（如DVD/蓝光）
        # item["version"] = ",".join(v for v in response.xpath('//div[@id="tmmSwatches"]/ul//a/span[@aria-label]/text()').getall())

        # # 3. 获取影评信息(每个电影获取最多5条)
        # reviews = []
        # review_block = response.xpath('//div/h3[@data-hook="dp-local-reviews-header"]/ancestor::div/ancestor::div//div[contains(@class, "a-row")]//ul')

        # count = 0
        # for review in review_block.xpath('.//li[@data-hook="review"]'):
        #     if count >= 5:
        #         break
        #     review_dict = {}
        #     review_dict["profile_name"] = review.xpath('.//span[@class="a-profile-name"]/text()').get(default="").strip()

        #     review_dict["review_rating"] = review.xpath('.//i[@data-hook="review-star-rating"]/span/text()').get(default="").strip().split(" out ")[0]  # eg. 3.0 out of 5 stars

        #     review_dict["review_title"] = review.xpath('.//a[@data-hook="review-title"]/span/text()').get(default="").strip()

        #     review_dict["review_date"] = review.xpath('.//span[@data-hook="review-date"]/text()').get(default="").strip().split(" on ")[-1]  # eg. Reviewed in the United States on January 1, 2020

        #     review_dict["review_text"] = " ".join(review.xpath('.//span[@data-hook="review-body"]/div//text()').getall()).strip()

        #     helpness = review.xpath('.//span[@data-hook="helpful-vote-statement"]/text()').get(default="").strip().replace("One person", "1 people").split(" people")[0] # eg. "2 people found this helpful" or "One person found this helpful"

        #     review_dict["review_helpness"] = helpness if helpness.isdigit() else "0"

        #     reviews.append(review_dict)
        #     count += 1

        # item["reviews"] = reviews

        # yield item