from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, BIGINT, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry
from datetime import datetime
import json
import settings
import pdb


DeclarativeBase = declarative_base()

def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(URL(**settings.DATABASE))

def create_table(engine):
    """"""
    DeclarativeBase.metadata.create_all(engine)

class ItemPipeline(object):
    """pipeline for storing scraped items in the database"""
    def __init__(self):
        """
        Initializes database connection and sessionmaker.
        Creates table.
        """
        engine = db_connect()
        create_table(engine)
        self.Session = sessionmaker(bind=engine)

    def process_item(self, data, keywords):
        """Save  in the database.
        This method is called for every item pipeline component.
        """
        session = self.Session()
        item = {}
        if not 'limit' in data.keys():
            try:
                item['longitud']    =   data['coordinates']['coordinates'][0]
                item['latitude']    =   data['coordinates']['coordinates'][1]
            except: pass
            try:
                item['created_at']  =   data['created_at']
            except:
                pass
            try:
                item['favorite_count']  =   data['favorite_count']
                item['retweet_count']   =   data['retweet_count']
            except:
                pass
            try:
                item['filter_level']    =   data['filter_level']
            except: pass
            try:
                item['in_reply_to'] =   data['in_reply_to']
                item['lang']    =   data['lang']
            except: pass
            try:
                item['placeCompleto']   =   json.dumps(data['place'])
                item['bounding']        =   json.dumps(json.loads(json.dumps(data['place']))['bounding_box'])
                item['place']           =   json.loads(json.dumps(data['place']))['full_name']
            except: pass
            try:
                item['possibly_sensitive']  =   data['possibly_sensitive']
                item['text']    =   data['text']
                item['keyword']    =  ';'.join(keywords)
                item['user_id'] =   data['user']['id']
                item['username'] =   data['user']['name']
                item['usertweet']    =   json.dumps(data['user'])
                item['tweet_id']    =   data['id']
            except: 
                pass

        try:
            session.add(tweet(**item))
            session.commit()
            print '+1'
        except:
            session.rollback()
            raise
        finally:
            item.clear()
            session.close()

    def process_log(self, len, key):
        """Save  in the database.
        This method is called everytime a loop is executed.
        """
        session = self.Session()
        log = {}
        log['rundate']  =   str(datetime.now())
        log['keyword']  =   key
        log['harvestedthisrun'] =   len
        try:
            session.add(LogPostgre(**log))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            log.clear()
            session.close()
        return

class tweet(DeclarativeBase):
    """Sqlalchemy deals model"""
    __tablename__ = "temp_tweets"
    
    id = Column(Integer, primary_key=True)
    longitud    =   Column('longitud', Float,   nullable= True) 
    latitude    =   Column('latitude', Float,   nullable=True)
    geom        =   Column('geom',Geometry('POINT') , nullable = True)
    geom_polygon        =   Column('geom_polygon',Geometry('POLYGON') , nullable = True)
    created_at  =   Column('created_at', DateTime(timezone=True))
    favorite_count  =   Column('favorite_count', Integer)
    retweet_count   =   Column('retweet_count', Integer)
    filter_level    =   Column('filter_level', String)
    in_reply_to     =   Column('in_reply_to', String, nullable = True) 
    lang    =   Column('lang', String, nullable=True)
    placeCompleto = Column('placeCompleto', String, nullable=True)
    bounding = Column('bounding', String , nullable = True)
    place   =   Column('place', String, nullable=True)
    possibly_sensitive   =  Column('possibly_sensitive', String, nullable=True)
    text  = Column('text', String)
    keyword =   Column('keyword', String, nullable=True)
    user_id = Column('user_id', BIGINT)
    username  = Column('username', String)
    usertweet = Column('usertweet', Text)
    tweet_id    =   Column('tweet_id', String)

class LogPostgre(DeclarativeBase):
    """Sqlalchemy deals model"""
    __tablename__ = "tweet_log"
    
    id = Column(Integer, primary_key=True)
    rundate = Column('rundate', DateTime)
    keyword = Column('keyword', String, nullable=True)
    harvestedthisrun = Column('harvestedthisrun', Integer, nullable=True)