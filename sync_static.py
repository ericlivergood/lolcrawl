from riotapi import RiotAPI

from postgres_adapter import PostgresAdapter

api_key = 'f009af7e-a9a8-4079-9579-8058dff394ed'

db = PostgresAdapter('dbname=lolstats')


api = RiotAPI(api_key)

champs = api.champion()

for c in champs['data'].keys():
    db.sync_champion(str(champs['data'][c]['id']), str(champs['data'][c]['name']))