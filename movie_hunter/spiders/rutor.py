import random
import re
import scrapy
from datetime import datetime, timezone, date, timedelta
from time import sleep
from typing import Optional

import dateutil.parser
from scrapy.exceptions import DropItem, CloseSpider


class RutorSpider(scrapy.Spider):
    name = "rutor"
    close_down_with_reason = None

    def start_requests(self):
        # urls = [
        #     'http://rutor.info/browse/0/1/0/0',
        #     'http://rutor.info/browse/1/1/0/0',
        #     'http://rutor.info/browse/2/1/0/0',
        #     'http://rutor.info/browse/3/1/0/0',
        #     'http://rutor.info/browse/4/1/0/0',
        # ]
        urls = []

        # range must be +1 to last page number
        for url in range(12):
            urls.append(scrapy.Request(url=f'http://rutor.info/browse/{url}/1/0/0', callback=self.parse))

        return urls

    def parse(self, response):
        pattern = re.compile('.+ / ([^/]+) \((\\d{4})\).*')

        for download_article in response.xpath('//div[@id="index"]/table/tr[not(@class="backgr")]'):
            # print(download_article.get())

            # if self.close_down_with_reason:
            #     raise CloseSpider(reason=self.close_down_with_reason)

            url = download_article.xpath('./td[2]/a[3]/@href').get()
            url = response.urljoin(url)

            title = download_article.xpath('./td[2]/a[3]/text()').get()

            seen_date = download_article.xpath('./td[1]/text()').get()
            seen_date = self.parse_date(seen_date)

            try:
                movie_title, year = pattern.match(title).groups()
                # sleep(random.uniform(0.0, 2.0))
                yield {
                    'title': movie_title,
                    'year': year,
                    'url': url,
                    'seen_date': seen_date,
                }
            except AttributeError:
                print(title)

        # next_page = response.css('li.next a::attr(href)').get()
        # if next_page is not None:
        #     next_page = response.urljoin(next_page)
        #     yield scrapy.Request(next_page, callback=self.parse)

    @staticmethod
    def parse_date(date_string: Optional[str]) -> Optional[datetime]:

        _date = None
        date_str = date_string.strip().lower()
        date_str = date_str.replace('z', '')  # Иногда бывает Z на конце "2017-03-06T08:07:52Z"
        date_str = date_str.replace(',', '')
        date_str = date_str.replace('года', '')
        date_str = date_str.replace('-', ' ')

        # STAGE 2
        months = [
            ('вчера', str(date.today() - timedelta(days=1))),
            ('сегодня', str(date.today())),
            ('янв(\\w+)?', 'jan'),
            ('фев(\\w+)?', 'feb'),
            ('мар(\\w+)?', 'mar'),
            ('апр(\\w+)?', 'apr'),
            ('ма\\w', 'may'),
            ('июн(\\w+)?', 'jun'),
            ('июл(\\w+)?', 'jul'),
            ('авг(\\w+)?', 'aug'),
            ('сен(\\w+)?', 'sep'),
            ('окт(\\w+)?', 'oct'),
            ('ноя(\\w+)?', 'nov'),
            ('дек(\\w+)?', 'dec'),
        ]
        for word, subst in months:
            date_str = re.sub(word, subst, date_str)

        # STAGE 3
        try:
            # Таймзону просто отбрасывам, так как ещё ни на одном сайте она не использовалась правильно.
            _date = dateutil.parser.parse(date_str, ignoretz=True)

            return _date
        except ValueError:
            pass

        return _date.replace(microsecond=0) if _date else None
