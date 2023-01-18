from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, ForeignKey, Integer, String, Boolean, Interval, Enum, Date, Float, Table, BigInteger, \
    inspect, create_engine

import pandas as pd

SQL_URL = "sqlite:///./database.db"

Base = declarative_base()

engine = create_engine(SQL_URL, connect_args={"check_same_thread": False}, echo=False)


genres_association_table = Table("genres_association_table",
    Base.metadata,
    Column("title_id", ForeignKey("basics_titles.id")),
    Column("genre_id", ForeignKey("genres.id")),)


class BasicsTitle(Base):
    """Contains the following information for titles"""

    __tablename__ = 'basics_titles'

    id = Column(Integer, primary_key=True, unique=True, nullable=False, comment='alphanumeric unique identifier of the title')
    titleType = Column(String, comment='the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)')
    primaryTitle = Column(String, comment='the more popular title / the title used by the filmmakers on promotional materials at the point of release')
    originalTitle = Column(String, comment='original title, in the original language')
    isAdult = Column(Boolean,  comment='0: non-adult title; 1: adult title')
    startYear = Column(Integer,  comment='represents the release year of a title. In the case of TV Series, it is the series start year')
    endYear = Column(Integer, nullable=True, comment='TV Series end year. \\N for all other title types')
    runtimeMinutes = Column(Integer, comment='Column primary runtime of the title, in minutes')

    genres = relationship("Genre", secondary=genres_association_table)

    def __repr__(self):
        return f'<Title Model: id = {self.id}, titleType = {self.titleType}, primaryTitle = {self.primaryTitle}, ' \
               f'isAdult = {self.isAdult}, startYear = {self.startYear}, endYear = {self.endYear}, ' \
               f'runtimeMinutes= {self.runtimeMinutes}, genres = {self.genres}>'


class Rating(Base):
    """Contains the IMDb rating and votes information for titles"""

    __tablename__ = 'ratings'

    id = Column(Integer, primary_key=True)
    title_id = Column(ForeignKey("basics_titles.id"), comment='alphanumeric unique identifier of the title')
    averageRating = Column(Float, comment='weighted average of all the individual user ratings')
    numVotes = Column(Integer, comment='number of votes the title has received')

    def __str__(self):
        return f'{self.averageRating}({self.numVotes})'

    def __repr__(self):
        return self.__str__()

class Episode(Base):
    """Contains the tv episode information"""

    __tablename__ = 'episodes'

    id = Column(Integer, primary_key=True, )
    self_id = Column(ForeignKey('basics_titles.id'), comment='alphanumeric identifier of episode')
    parent_id = Column(ForeignKey('basics_titles.id'), comment='alphanumeric identifier of the parent TV Series')
    seasonNumber = Column(Integer, comment='season number the episode belongs to')
    episodeNumber = Column(Integer, comment='episode number of the tconst in the TV series')


class Genre(Base):
    """Contains genre information"""

    __tablename__ = 'genres'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    def __repr__(self):
        return self.name

class TitlesAkas(Base):

    __tablename__ = 'akas'

    id = Column(Integer, primary_key=True)
    title_id = Column(ForeignKey('basics_titles.id'), comment='a tconst, an alphanumeric unique identifier of the title')
    ordering = Column(Integer, comment='a number to uniquely identify rows for a given titleId')
    title = Column(String, comment='the localized title')
    region = Column(String, comment='the region for this version of the title')
    language = Column(String, comment=' the language of the title')
    isOriginalTitle = Column(Boolean, comment='0: not original title; 1: original title')

    types = Column(String)  #TODO вот это вот надоб переделать в m2m
    attributes = Column(String)

    def __str__(self):
        return f'[{self.title_id}] - {self.title}({self.region}/{self.language})'

    def __repr__(self):
        return self.__str__()

    #TODO так то и из тайтлов нужно убрать названия и сделать их частью этой таблицы.....