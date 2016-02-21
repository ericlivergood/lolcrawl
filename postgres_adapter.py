import psycopg2
from datetime import datetime

class PostgresAdapter(object):

    def __init__(self, conn_string):
        self.conn = psycopg2.connect(conn_string)
        self.sql = self.conn.cursor()

    def _scalar(self, x):
        return x[0]

    def get_summoners(self): 
        self.sql.execute('select summoner_id from summoner_history where matches_parsed_on is null limit 10;')
        return map(self._scalar, self.sql)

    def get_matches(self):
        self.sql.execute('select match_id from match_history where match_json is null limit 1000;')
        return map(self._scalar, self.sql)        


    def add_summoner(self, id):
        self.sql.execute("""
            insert into summoner_history(summoner_id) 
            SELECT %s
            WHERE NOT EXISTS(
                select 1
                from summoner_history h
                where h.summoner_id = %s
            )
            """, [id,id])
        self.conn.commit()
        return 

    def add_match(self, id):
        self.sql.execute("""
            insert into match_history(match_id) 
            SELECT %s
            WHERE NOT EXISTS(
                select 1
                from match_history h
                where h.match_id = %s
            )
            """, [id,id])
        self.conn.commit()
        return        

    def crawl_match(self, id, match_json):
        date = datetime.now()

        self.sql.execute("""
            update match_history
            set 
                match_json = %s
            ,   added_on = %s
            where match_id = %s
            """, [psycopg2.Binary(match_json), date, id])
        self.conn.commit()     
        return 
    
    def crawl_summoner(self, id, history_json):
        date = datetime.now()

        self.sql.execute("""
            update summoner_history
            set 
                summoner_history_json = %s
            ,    last_refreshed_on = %s
            ,    matches_parsed_on = %s
            where summoner_id = %s
            """, [psycopg2.Binary(history_json), date, date, id])
        self.conn.commit()        
        return 

    def sync_champion(self, id, name):
        self.sql.execute("""
            insert into champions(champion_id, name)
            values(%s, %s)
            on conflict(champion_id) do update set name = %s;
        """, [id, name, name])

        self.conn.commit()
        return