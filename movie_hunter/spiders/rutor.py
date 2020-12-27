import re
from datetime import date, datetime, timedelta
from typing import Optional

import dateutil.parser
import scrapy

from movie import LastUpdate, db_connect


class RutorSpider(scrapy.Spider):
    name = "rutor"
    start_urls = [
        'http://rutor.info/browse/0/1/0/0',
    ]

    def __init__(self, *args, **kwargs):
        """
        Initializes database connection
        """

        super().__init__(*args, **kwargs)

        self.session = db_connect()
        print("**** RutorSpider: database connected ****")

        self.last_update = self.session.query(LastUpdate).first().last_update
        print(f"**** Last update: {self.last_update} ****")
        self.last_seen_date = None

        # self.file = open('A:\\movies.txt', mode='w', encoding='utf-8')

    # def start_requests(self):
    #     # urls = [
    #     #     'http://rutor.info/browse/0/1/0/0',
    #     #     'http://rutor.info/browse/1/1/0/0',
    #     #     'http://rutor.info/browse/2/1/0/0',
    #     #     'http://rutor.info/browse/3/1/0/0',
    #     #     'http://rutor.info/browse/4/1/0/0',
    #     # ]
    #     urls = []
    #
    #     # range must be +1 to last page number
    #     for url in range(12):
    #         urls.append(scrapy.Request(url=f'http://rutor.info/browse/{url}/1/0/0', callback=self.parse))
    #
    #     return urls

    def parse(self, response):
        pattern = re.compile('.+ / ([^/]+) \((\\d{4})\).*')
        is_this_last_page = False

        for download_article in response.xpath('//div[@id="index"]/table/tr[not(@class="backgr")]'):
            # print(download_article.get())

            seen_date = download_article.xpath('./td[1]/text()').get()
            seen_date = self.parse_date(seen_date)

            if seen_date < self.last_update:
                is_this_last_page = True
                continue

            url = download_article.xpath('./td[2]/a[3]/@href').get()
            url = response.urljoin(url)

            title = download_article.xpath('./td[2]/a[3]/text()').get()

            try:
                movie_title, year = pattern.match(title).groups()

                yield {
                    'title': movie_title,
                    'year': year,
                    'url': url,
                    'seen_date': seen_date,
                }
            except AttributeError:
                print(f'*** Something wrong during parsing movie: {title} ***')

        # GO TO NEXT PAGE (IF NEEDED)
        next_page = response.xpath('/html/body/div[2]/div[1]/p[2]/a[last()]/@href').get()
        next_page = response.urljoin(next_page)
        if not is_this_last_page and next_page is not None:
            next_page = response.urljoin(next_page)
            print(f'*** Going to the next page: {next_page} ***')
            yield scrapy.Request(next_page, callback=self.parse)

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
