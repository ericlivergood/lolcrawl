from crawler import MatchCrawler
from postgres_adapter import PostgresAdapter

api_key = 'f009af7e-a9a8-4079-9579-8058dff394ed'

db = PostgresAdapter('dbname=lolraw')

c = MatchCrawler(api_key, db)
c.crawl()