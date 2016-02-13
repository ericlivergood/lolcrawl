import requests
from datetime import datetime
from time import sleep


class RateLimitException(Exception):
    pass

class RiotAPIException(Exception):
    pass

class RiotAPI(object):

    def __init__(self, api_key):
        self._baseurl = 'https://na.api.pvp.net/api/lol/na/v2.2/'
        self._last_request = datetime(1900, 1, 1)
        self._api_key = api_key

    def _process_response(self, resp):
        if(resp.status_code == 200):
            return resp.json()  
        elif(resp.status_code == 429):
            raise RateLimitException()
        else:
            raise RiotAPIException('Invalid request.  Status Code: ' + str(resp.status_code)+ '\n' + resp.url)

    def _check_rate_limit(self):
        since_last_request = (datetime.now() - self._last_request).total_seconds()
        next_request_in = 1.2 - since_last_request
        if(next_request_in > 0):
            sleep(next_request_in)

        self._last_request = datetime.now()

    def _make_request(self, url):
        self._check_rate_limit()
        params = { 'api_key': self._api_key }
        r = requests.get(url, params=params)
        return self._process_response(r)

    def matchlist(self, summoner_id):
        return self._make_request(self._baseurl + 'matchlist/by-summoner/'+summoner_id)


    def match(self, match_id):
        return self._make_request(self._baseurl + 'match/'+match_id)