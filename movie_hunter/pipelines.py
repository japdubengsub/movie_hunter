# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from movie import LastUpdate, Movie


class MovieHunterPipeline:

    def __init__(self):
        """
        Initializes database connection
        """
        # self.session = db_connect()
        # print("**** MovieHunterPipeline: database connected ****")
        #
        # self.last_update = self.session.query(LastUpdate).first().last_update
        # print(f"**** Last update: {self.last_update} ****")
        # self.last_seen_date = None

        self.file = open('A:\\movies.txt', mode='w', encoding='utf-8')

    def close_spider(self, spider):
        last = spider.session.query(LastUpdate).first()
        last.last_update = spider.last_seen_date
        spider.session.add(last)
        spider.session.commit()
        self.file.close()

    def process_item(self, item, spider):
        if item['seen_date'] < spider.last_update:
            return item

        if not spider.last_seen_date or spider.last_seen_date < item['seen_date']:
            spider.last_seen_date = item['seen_date']

        exist_movie: Movie = spider.session.query(Movie).filter_by(
            title=item['title'],
            year=item['year']
        ).one_or_none()

        # UPDATE STATS FOR EXISTING MOVIE
        if exist_movie is not None:
            if item['seen_date'] > spider.last_update:
                exist_movie.count += 1
                if exist_movie.last_seen < item['seen_date']:
                    exist_movie.last_seen = item['seen_date']
                spider.session.add(exist_movie)
                spider.session.commit()

                # raise DropItem(f"Duplicate item found: {item['title']} ({item['year']})")

        # NEW MOVIE
        else:
            new = Movie(item['title'], item['year'], 1, last_seen=item['seen_date'])
            spider.session.add(new)
            spider.session.commit()

            # write URL to file
            rym_search_url = f'https://rateyourmusic.com/search?searchtype=F&searchterm={item["title"]} {item["year"]}'
            self.file.writelines([
                item['url'],
                '\n',
                rym_search_url.replace(' ', '%20'),
                '\n',
            ])

            return item
