import pickle, psycopg2
import datetime 
raw = psycopg2.connect('dbname=lolraw')
stat = psycopg2.connect('dbname=lolstats')


def _scalar(x):
    return x[0]

def get_matches():
    sql = raw.cursor()
    sql.execute('select match_json from match_history where match_json is not null limit 1000;')
    return map(_scalar, sql)   


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
                where h.match_id = %s
            )
            """, [match_id, datetime.datetime.now(), start, duration, region, season, patch, match_id])   

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

def _mark_match_parsed(match_id):
    date = datetime.datetime.now()
    sql = raw.cursor()
    sql.execute("""
        update match_history
        set 
            parsed_on = %s
        where match_id = %s
        """, [date, match_id])
    raw.commit()    

def parse_match(m):
    match = pickle.loads(m)
    id = str(match['matchId'])
    if(match['queueType'] == 'RANKED_SOLO_5x5' or 1==1):
        print('adding match ' + id)
        start = datetime.datetime.fromtimestamp(match['matchCreation']/1000.0)
        _upsert_match(id, start, match['matchDuration'], match['region'], match['season'], match['matchVersion'])

        for t in match['teams']:
            _upsert_team(id, str(t['teamId']), t['baronKills'], t['dragonKills'], t['firstBlood'], t['winner'])


        for p in match['participantIdentities']:
            participant = match['participants'][p['participantId']-1]
            summoner_id = str(p['player']['summonerId'])
            _upsert_summoner(summoner_id, p['player']['summonerName'])
            _upsert_summoner_match(id, summoner_id, participant['teamId'], participant['championId'], participant['highestAchievedSeasonTier'])

        stat.commit()

    _mark_match_parsed(id)


def run():
    matches = get_matches()
    while(len(matches) > 0):
        for m in matches:
            parse_match(m)
        matches = get_matches()


run()
            
