import os
from datetime import datetime, date
from typing import Dict, List, Tuple

from sqlalchemy import Column, Integer, DateTime, NVARCHAR, CHAR, PrimaryKeyConstraint, SmallInteger
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


def db_connect():
    db_file = r'movie_hunter.sqlite'

    # engine = create_engine('sqlite:///:memory:', echo=True)
    engine = create_engine('sqlite:///' + db_file, echo=False)

    # if not os.path.isfile(db_file):
    #     Base.metadata.create_all(engine)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    return session


class Movie(Base):
    __tablename__ = 'movies'

    title = Column(NVARCHAR(1000), nullable=False)
    year = Column(SmallInteger, nullable=False)
    count = Column(SmallInteger, nullable=False)
    last_seen = Column(DateTime, nullable=False)

    PrimaryKeyConstraint(year, title)

    def __init__(self, title: str, year: int, count: int, last_seen: date = None):
        self.title = title
        self.year = year
        self.count = count
        self.last_seen = last_seen or date.today()


class LastUpdate(Base):
    __tablename__ = 'last_update'

    last_update = Column(DateTime, primary_key=True)

    def __init__(self, title: str, year: int, count: int):
        self.title = title
        self.year = year
        self.count = count
        self.last_seen = date.today()
