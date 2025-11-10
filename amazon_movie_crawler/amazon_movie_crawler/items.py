# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class AmazonMovieItem(scrapy.Item):
    product_id = scrapy.Field()  # 对应B00006HAXW
    source_url = scrapy.Field()  # 商品页URL（https://amazon.com/dp/{product_id}）
    html_path = scrapy.Field()  # 原始HTML存储路径
    release_date = scrapy.Field()  # 上映时间
    genre = scrapy.Field()  # 电影风格
    director = scrapy.Field()  # 导演
    actors = scrapy.Field()  # 全体演员(","隔开)
    version = scrapy.Field()  # 版本（蓝光/DVD/英文/中文等）
    movie_name = scrapy.Field()  # 电影名（用于去重）
    reviews = scrapy.Field()  # 影评列表(使用json字符串)