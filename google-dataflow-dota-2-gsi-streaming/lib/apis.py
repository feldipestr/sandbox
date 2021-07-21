import os
import requests
from collections import deque


class ApiKeys(object):
    def __init__(self, **kwargs):
        self.current_key = ''
        # 100 SteamAPI keys are more than enough (1 SteamAPI key is enough to hit 1 request per second)
        self.keys_rotator = deque(maxlen=100)
        self.key_name = kwargs.get('key_name', 'key')

        keys_list = kwargs.get('keys_list')
        keys_file_path = kwargs.get('keys_file_path', '')

        if keys_list:
            self.load_keys_list(keys_list)
        elif keys_file_path:
            self.load_keys_file(keys_file_path)

    def get_current_key(self):
        return self.current_key

    def get_key_name(self):
        return self.key_name

    def get_next_key(self):
        key_value = self.keys_rotator.popleft()

        if not key_value:
            key_value = ''

        self.current_key = key_value

        self.keys_rotator.append(key_value)

        return self.get_current_key()

    def load_keys_list(self, keys_list=[]):
        if keys_list:
            self.keys_rotator.clear()

        for key in keys_list:
            key_value = key if key else ''
            self.keys_rotator.append(key_value)

            if not self.current_key:
                self.current_key = key_value

    def load_keys_file(self, file_path=''):
        if os.path.isfile(file_path):
            with open(file_path, 'r') as f:
                for line in f:
                    key = line.rstrip('\n')
                    key_value = key if key else ''
                    self.keys_rotator.append(key_value)

                    if not self.current_key:
                        self.current_key = key_value


class SteamApiKeys(ApiKeys):
    def __init__(self, **kwargs):
        kwargs['keys_file_path'] = 'steam_api_keys.txt'
        super(SteamApiKeys, self).__init__(**kwargs)


class SourceApi(object):
    def __init__(self, **kwargs):
        self.api_keys = kwargs.get('api_keys', ApiKeys(**kwargs))
        self.request_timeout = kwargs.get('request_timeout', 1)

    def send_request(self, url):
        result_json = {}
        error_msg = ''

        try:
            result = requests.get(url, timeout=self.request_timeout)
            status_code = result.status_code
            result_json = result.json()
        except requests.exceptions.Timeout:
            status_code = 408
            error_msg = '408: Request timeout!'
        except requests.exceptions.RequestException as e:
            status_code = 500
            error_msg = f'500, Request error: {e}!'
        except ValueError as e:
            status_code = 500
            error_msg = f'500, Value error: {e}!'

        return status_code == 200, result_json, error_msg

    def get_request_result(self, url='', try_without_key=True):
        api_key_name = self.api_keys.get_key_name()
        api_key = self.api_keys.get_next_key()
        api_key_url = ''

        if api_key:
            api_key_url = f'{"&" if "?" in url else "?"}{api_key_name}={api_key}'

        route_url = url

        if try_without_key:
            res, res_json, err = self.send_request(route_url)
            if res:
                return True, res_json, err

        route_url += api_key_url
        res, res_json, err = self.send_request(route_url)

        return res, res_json, err


# Getting live matches (games) data from Steam REST API rotating API keys
class SteamApi(object):
    def __init__(self, **kwargs):
        kwargs['api_keys'] = SteamApiKeys(**kwargs)
        kwargs['request_timeout'] = 1
        self.api = SourceApi(**kwargs)

    def get_live_matches(self):
        live_matches = []

        url = 'https://api.steampowered.com/IDOTA2Match_570/GetLiveLeagueGames/V001/?format=json'
        res, res_json, error_msg = self.api.get_request_result(url)

        if res and isinstance(res_json, dict):
            result_dict = res_json.get('result', {})
            games_list = result_dict.get('games', [])
            for game_data in games_list:
                live_match_id = game_data.get('match_id')

                if live_match_id is not None:
                    if live_match_id > 0:
                        live_match_data = {'match_id': live_match_id,
                                           'attributes': {}}

                        radiant_team = game_data.get('radiant_team')
                        dire_team = game_data.get('dire_team')

                        if isinstance(radiant_team, dict):
                            live_match_data['attributes']['radiant_team_name'] = radiant_team.get('team_name')

                        if isinstance(dire_team, dict):
                            live_match_data['attributes']['dire_team_name'] = dire_team.get('team_name')

                        if live_match_data['attributes'].keys():
                            live_matches.append(live_match_data)

        return live_matches


# Singleton
class SteamAPIConnection(object):
    class __SteamAPIConnection:
        def __init__(self, **kwargs):
            self.client = SteamApi(**kwargs)

    instance = None

    def __new__(cls, **kwargs):
        if not SteamAPIConnection.instance:
            SteamAPIConnection.instance = SteamAPIConnection.__SteamAPIConnection(**kwargs)
        return SteamAPIConnection.instance
