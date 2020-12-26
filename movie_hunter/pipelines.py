# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import random
import subprocess
from time import sleep

from scrapy.exceptions import DropItem, CloseSpider

from movie import Movie, db_connect, LastUpdate


class MovieHunterPipeline:

    def __init__(self):
        """
        Initializes database connection
        """
        self.session = db_connect()
        print("**** MovieHunterPipeline: database connected ****")

        self.last_update = self.session.query(LastUpdate).first().last_update
        print(f"**** Last update: {self.last_update} ****")
        self.last_seen_date = None

        self.file = open('A:\\movies.txt', mode='w', encoding='utf-8')

    def close_spider(self, spider):
        last = self.session.query(LastUpdate).first()
        last.last_update = self.last_seen_date
        self.session.add(last)
        self.session.commit()
        self.file.close()

    def process_item(self, item, spider):
        if item['seen_date'] < self.last_update:
            return item

        if not self.last_seen_date or self.last_seen_date < item['seen_date']:
            self.last_seen_date = item['seen_date']

        exist_movie: Movie = self.session.query(Movie).filter_by(
            title=item['title'],
            year=item['year']
        ).one_or_none()

        if exist_movie is not None:
            if item['seen_date'] > self.last_update:
                exist_movie.count += 1
                if exist_movie.last_seen < item['seen_date']:
                    exist_movie.last_seen = item['seen_date']
                self.session.add(exist_movie)
                self.session.commit()

                # raise DropItem(f"Duplicate item found: {item['title']} ({item['year']})")

            # else:
            #     spider.close_down_with_reason = f"Seen date is too far: {item['seen_date']} ({self.last_update})"
            # raise CloseSpider(f"Seen date is too far: {item['seen_date']} ({self.last_update})")
            # session.close()
        else:
            new = Movie(item['title'], item['year'], 1, last_seen=item['seen_date'])
            self.session.add(new)
            self.session.commit()

            # write URL to file
            self.file.write(item['url'])
            self.file.write('\n')

            return item
            # session.close()

        # return item
