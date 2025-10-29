# ETL(Extract Transform Load) 作业要求
- **数据来源**：http://snap.stanford.edu/data/web-Movies.html 
- **ETL要求**：
  1. 获取用户评价数据中的7,911,684个用户评价
  2. 从Amazon网站中利用网页中所说的方法利用爬虫获取253,059个Product信息页面
  3. 挑选其中的电影页面，通过ETL从数据中获取
     - 电影ID，评论用户ID
     - 评论用户ProfileName，评论用户评价Helpfulness，评论用户Score，评论时间Time，评论结论Summary，评论结论Tex
     - 电影上映时间，电影风格，电影导演，电影主演，电影演员，电影版本
  4. 在网页中不同网页可能是相同的电影（如同一部电影的蓝光、DVD版本，同一部电影的不同语言的版本等），通过ETL对相同的电影（需要给出你所认为的相同的定义）进行合并
  5. 在网页中电影演员、电影导演、电影主演等会出现同一个人但有不同名字的情况（如middle name，名字缩写等），通过ETL对相同的人名进行合并
  6. 在网页中部分电影没有上映时间，可以通过第三方数据源（如IMDB、豆瓣等）或者从评论时间来获取
  7. 通过ETL工具存储Amazon页面和最终合并后的电影之间的数据血缘关系，即可以知道某个电影的某个信息是从哪些网站或者数据源获取的，在合并的过程中最终我们采用的信息是从哪里来的。
- **可以参考的工具**：
  1. **ETL工具**：见《ETL工具使用介绍》
  2. **Web爬虫**：https://scrapy.org

# ETL工具使用介绍
## Pentaho DI工具介绍：
1. 官方tutorial：https://www.hitachivantara.com/en-us/products/pentaho-platform/data-integration-analytics/pentaho-tutorials.html（链接到外部网站）。
2. 中文网站：http://www.kettle.org.cn/（链接到外部网站）。
3. YouTube视频：
   - https://www.youtube.com/watch?v=lD2zumIyt9Q&t=14s（链接到外部网站）。
   - https://www.youtube.com/watch?v=-heDV0ZwR3w&list=PLT8E937iDWrwOj8dGMptTazyfqEjZTUt6（链接到外部网站）。


## Airflow工具介绍：
1. 官方文档：https://airflow.apache.org/docs/apache-airflow/stable/index.html（链接到外部网站）。
2. YouTube视频：https://www.youtube.com/watch?v=eZfD6x9FJ4E