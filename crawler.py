from riotapi import RiotAPI, RiotAPIException, MatchNotFoundException

class MatchCrawler(object):
    def __init__(self, api_key, persistence_adapter):
        self.api = RiotAPI(api_key)
        self.db = persistence_adapter

    def _process_participant(self, p):
        self.db.add_summoner(str(p['player']['summonerId']))

    def _process_match(self, match):
        id = str(match['matchId'])
        self.db.add_match(id)

    def crawl(self):
        summoners = self.db.get_summoners()
        matches = self.db.get_matches()

        while(len(summoners) > 0 or len(matches) > 0):
            for s in summoners:
                print('crawling summoner ' + str(s))

                ml = self.api.matchlist(s)
                map(self._process_match, ml['matches'])
                self.db.crawl_summoner(s, str(ml))

            for m in matches:
                print('crawling match ' + m) 

                try:
                    data = self.api.match(m)
                    map(self._process_participant, data['participantIdentities'])
                    self.db.crawl_match(m, str(data))
                except RiotAPIException as e:
                    print e
                except MatchNotFoundException as e:
                    self.db.crawl_match(m, 'Invalid')
                           

            summoners = self.db.get_summoners()
            matches = self.db.get_matches()