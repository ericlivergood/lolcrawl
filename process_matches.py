import json, psycopg2
import datetime 
raw = psycopg2.connect('dbname=lolraw')
stat = psycopg2.connect('dbname=lolstat')


def _scalar(self, x):
    return x[0]

def get_matches():
    sql = raw.cursor()
    sql.execute('select match_json from match_history where match_json is not null limit 1000;')
    return map(self._scalar, self.sql)   


def _upsert_summoner(id, name):
    sql = stat.cursor()
    sql.execute("""
            insert into summoners(summoner_id ,name) 
            SELECT %s, %s
            WHERE NOT EXISTS(
                select 1
                from summoners h
                where h.summoner_id = %s
            )
            """, [id,name,id])    

def _upsert_summoner_match(match_id, summoner_id, team_id, champion_id, rank):
    sql = stat.cursor()
    sql.execute("""
            insert into summoner_matches(match_id, summoner_id, team_id, champion_id, rank) 
            SELECT %s, %s, %s, %s, %s
            WHERE NOT EXISTS(
                select 1
                from summoner_matches h
                where h.summoner_id = %s
                and h.match_id = %s
            )
            """, [match_id, summoner_id,team_id, champion_id, rank, summoner_id, match_id])        

def _upsert_match(match_id, start, duration, region, season, patch):
    sql = stat.cursor()
    sql.execute("""
            insert into matches(match_id, parsed_on, started_on, duration, region, season, patch) 
            SELECT %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS(
                select 1
                from matches h
                where h.summoner_id = %s
            )
            """, [match_id, datetime.datetime.now(), duration, region, season, patch, match_id])   

def _upsert_team(match_id, team_id, barons, dragons, first_blood, winner):
    sql = stat.cursor()
    sql.execute("""
            insert into teams(match_id, team_id, barons, dragons, first_blood, winner)
            SELECT %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS(
                select 1
                from teams h
                where h.match_id = %s
                and h.team_id = %s
            )
            """, [match_id, team_id, barons, dragons, first_blood, winner, match_id, team_id])

def parse_match(m):
    match = json.loads(m)

    if(match['queue_type'] == 'RANKED_SOLO_5x5'):
        id = match['matchId']
        start = datetime.datetime.fromtimestamp(match['matchCreation']/1000.0)
        _upsert_match(id, start, match['matchDuration'], match['region'], match['season'], match['matchVersion'])

        for t in match['teams']:
            _upsert_team(id, t['teamId'], team['baronKills'], team['dragonKills'], team['firstBlood'], team['winner'])


        for p in match['participantIdentities']:
            participant = match['participants'][p['participantId']]
            summoner_id = p['player']['summoner_id']
            _upsert_summoner(summoner_id, p['player']['summonerName'])
            _upsert_summoner_match(id, summoner_id, participant['teamId'], participant['championId'], participant['highestAchievedSeasonTier'])

        stat.commit()


def run():
    for m in get_matches():
        parse_match(m)
            
