from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from movie_hunter.spiders.rutor import RutorSpider

process = CrawlerProcess(get_project_settings())

process.crawl(RutorSpider)

process.start()
