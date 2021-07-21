import time
import json
from flask import jsonify
from google.cloud import datastore

project_id = '<GOOGLE-CLOUD-PROJECT-NAME>'

features = [
    "steamid",
    "assists",
    "camps_stacked",
    "commands_issued",
    "consumable_gold_spent",
    "deaths",
    "denies",
    "gold",
    "gold_from_creep_kills",
    "gold_from_hero_kills",
    "gold_from_income",
    "gold_from_shared",
    "gold_lost_to_death",
    "gold_reliable",
    "gold_spent_on_buybacks",
    "gold_unreliable",
    "gpm",
    "hero_damage",
    "item_gold_spent",
    "kill_streak",
    "kills",
    "last_hits",
    "net_worth",
    "runes_activated",
    "support_gold_spent",
    "wards_destroyed",
    "wards_placed",
    "wards_purchased",
    "xpm",
]


def json_response(data):
    response = jsonify(data)
    return response

# main function
def live_match_stat(request):
    def to_int(value, default: int = 0) -> int:
        int_value = default
        try:
            int_value = int(value)
        except (ValueError, TypeError):
            pass
        return int_value

    match_data = {}
    team_names = {'team2': 'radiant', 'team3': 'dire'}

    token = request.args.get('token', '')

    client = datastore.Client(project=project_id)

    query = client.query(kind='gsi-events')
    if token:
        query.add_filter('token', '=', token)

    entities = list(query.fetch(limit=1))

    if len(entities) <= 0:
        return json_response(match_data)

    players_data = {}
    teams_accounts = {'radiant': [], 'dire': []}

    try:
        raw_match_data = entities[0]["match_data"]
        event_data = json.loads(raw_match_data)
    except ValueError:
        event_data = {}

    if not event_data.keys():
        return json_response(match_data)

    timestamp = event_data.get('timestamp', 0)

    if timestamp > 0:
        age = int(time.time()) - timestamp
        match_data['event_age_seconds'] = age

    map_data = event_data.get('map')

    if isinstance(map_data, dict):
        match_data['match_id'] = to_int(map_data.get('matchid'), 0)
        match_data['clock_time'] = to_int(map_data.get('clock_time'), 0)
        match_data['win_team'] = map_data.get('win_team')

    player = event_data.get('player')

    if isinstance(player, dict):
        for team_key, team_name in team_names.items():
            team_data = player.get(team_key)

            if isinstance(team_data, dict):
                # each event has 20 random slots for both teams to store a player information
                for p in range(20):
                    player_name = f'player{p}'
                    player_n = team_data.get(player_name)

                    if isinstance(player_n, dict):
                        if not players_data.get(player_name):
                            players_data[player_name] = {}

                        for f in features:
                            f_val = player_n.get(f)
                            if f_val is None:
                                f_val = 0

                            if f == "commands_issued":
                                players_data[player_name]["apm"] = 0 \
                                    if match_data.get('clock_time', 0) == 0 \
                                    else int(60.0 * f_val / match_data['clock_time'])
                            else:
                                players_data[player_name][f] = f_val

                        player_name_en_val = player_n.get('name', '')
                        players_data[player_name]['player_name_en'] = player_name_en_val
                        players_data[player_name]['slot'] = p
                        players_data[player_name]['side'] = team_name
                        players_data[player_name]['team_name'] = player_n.get('team_name', '')

                        steam_id = players_data[player_name]['steamid']
                        if steam_id:
                            try:
                                account_id = int(steam_id) - 76561197960265728
                            except (TypeError, ValueError):
                                account_id = 0

                            if account_id:
                                players_data[player_name]['account_id'] = account_id
                                teams_accounts[team_name].append(account_id)

    items = event_data.get('items')
    if isinstance(items, dict):
        for team_key, team_name in team_names.items():
            team_data = items.get(team_key)

            if isinstance(team_data, dict):
                # each event has 20 random slots for both teams to store a player information
                for p in range(20):
                    player_name = f'player{p}'
                    player_n = team_data.get(player_name)

                    if isinstance(player_n, dict):
                        if not players_data.get(player_name):
                            players_data[player_name] = {}

                        for s in range(10):
                            slot_name = f'slot{s}'
                            slot_data = player_n.get(slot_name)

                            item_name = ''
                            if isinstance(slot_data, dict):
                                item_name = slot_data.get('name', '')

                            players_data[player_name][f'item_{slot_name}'] = item_name

    hero = event_data.get('hero')
    if isinstance(hero, dict):
        for team_key, team_name in team_names.items():
            team_data = hero.get(team_key)

            if isinstance(team_data, dict):
                # each event has 20 random slots for both teams to store a player information
                for p in range(20):
                    player_name = f'player{p}'
                    player_n = team_data.get(player_name)

                    if isinstance(player_n, dict):
                        if not players_data.get(player_name):
                            players_data[player_name] = {}

                        hero_name = player_n.get('name', '')
                        players_data[player_name]['player_hero'] = hero_name

                        level = player_n.get('level', -1)
                        players_data[player_name]['level'] = level

    if players_data.keys():
        match_data['players'] = players_data.values()

    clock_time_val = match_data['clock_time']
    mask = '%H:%M:%S' if clock_time_val >= 3600 else '%M:%S'
    match_data['clock_time'] = time.strftime(mask, time.gmtime(clock_time_val))

    return json_response(match_data)
