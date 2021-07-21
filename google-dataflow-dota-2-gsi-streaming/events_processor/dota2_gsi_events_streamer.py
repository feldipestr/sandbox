import os
import sys
import logging
import apache_beam as beam
from apache_beam.transforms import GroupByKey
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions, StandardOptions, WorkerOptions


# Validating messages if they are started with a special key and contain JSON context
class Validate(beam.DoFn):
    def process(self, msg: str, kind: str):
        import json
        import chardet

        element_data = {}

        if msg.startswith(f'{kind}='):
            msg = msg.replace(f'{kind}=', '')
            try:
                element_data = json.loads(msg.decode(chardet.detect(msg)["encoding"]))
            except ValueError:
                pass

        if element_data.keys():
            dumped = json.dumps(element_data)
            yield dumped


# Parsing messages and enriching with key attributes and timestamps
class Parse(beam.DoFn):
    def process(self, event_dump: str):
        import json

        # convert a value to the integer type
        def to_int(value, default: int = 0) -> int:
            int_value = default
            try:
                int_value = int(value)
            except (ValueError, TypeError):
                pass
            return int_value

        # First starting events can come without 'matchid' attribute.
        match_id = 0
        event_data = {}
        new_data = json.loads(event_dump)

        if new_data.keys():
            event_data = new_data

            auth = new_data.get('auth')
            if isinstance(auth, dict):
                token = auth.get('token')
                event_data['token'] = token if token else 'unknown'

            provider = new_data.get('provider')
            if isinstance(provider, dict):
                timestamp = provider.get('timestamp', 0)
                event_data['timestamp'] = timestamp

            map_data = new_data.get('map')
            if isinstance(map_data, dict):
                match_id = to_int(map_data.get('matchid', '0'), 0)
                clock_time = to_int(map_data.get('clock_time', '0'), 0)
                game_time = to_int(map_data.get('game_time', '0'), 0)

                event_data['match_id'] = match_id
                event_data['clock_time'] = 0 if clock_time < 0 else clock_time
                event_data['game_time'] = game_time

        if not event_data.get('clock_time'):
            event_data['clock_time'] = 0

        events_of_the_match = (match_id, event_data)

        yield events_of_the_match


# Extracting the last event for each token (spectator) from a single window of events.
# Enriching the last event by the team names. Team names are only available to be retrieved from SteamAPI.
# Making sure the the found (in the window) event is really the last one and should be stored.
class EnrichLastEvent(beam.DoFn):
    def process(self, events_of_the_match: tuple, project_id: str, kind_live_matches: str, kind_gsi_event: str):
        import json
        from lib.datastore import DatastoreDb

        key, events_list = events_of_the_match
        # There is only one live match entity in Google Datastore and its key is '0'.
        live_matches_entity = DatastoreDb(project_id=project_id, kind=kind_live_matches).query_entity('0')
        # If the entity with key='0' exists it would contain a list of live matches(games) from the SteamAPI.
        live_matches_dump = live_matches_entity.get('matches') if live_matches_entity else '{}'

        try:
            live_matches = json.loads(live_matches_dump)
        except ValueError:
            live_matches = {}

        live_matches_refresh = False

        # One single token means one particular spectator.
        all_tokens = set(m.get('token') for m in events_list if m.get('token') is not None)

        for token in all_tokens:
            # Find the maximum value of clock_time for all of the events (belong to a single window frame) of the token.
            max_clock_time = max(mm.get('clock_time')
                                 for mm in events_list
                                 if mm.get('token') == token
                                 and mm.get('clock_time') is not None)

            # Find the last event for the token.
            last_event_list = [e for e in events_list
                               if e.get('token') == token
                               and e.get('clock_time') == max_clock_time][:1]

            match_data = last_event_list[0] if last_event_list else {}

            if match_data.keys():
                # team2 is the name for 'radiant' side team in gsi events
                # team3 is the name for 'dire' side team in gsi events
                # radiant_team_name is an attribute name where radiant's team name is stored by SteamApi() (lib.apis.py)
                # dire_team_name is an attribute name where dire's team name is stored by SteamApi() (lib.apis.py)
                team_names = {'team2': 'radiant_team_name',
                              'team3': 'dire_team_name'}

                match_id = match_data.get('match_id', 0)
                token = match_data.get('token', 'unknown')
                game_time = match_data.get('game_time', 0)
                timestamp = match_data.get('timestamp', 0)
                clock_time = match_data.get('clock_time', 0)

                write_to_db = True

                # Make sure that the last event we've found is the really last one
                # that we haven't stored in Datastore yet.
                # Retrieving the last stored event from the Datastore for the token.
                existent_match_entity = DatastoreDb(project_id=project_id, kind=kind_gsi_event).query_entity(token)

                # Comparing timestamps for the last events (stored one and found one).
                e_game_time = existent_match_entity.get('game_time', 0) if existent_match_entity else 0
                e_timestamp = existent_match_entity.get('timestamp', 0) if existent_match_entity else 0
                e_match_id = existent_match_entity.get('match_id', 0) if existent_match_entity else 0

                if e_game_time > game_time and e_match_id == match_id:
                    write_to_db = False

                if e_timestamp > timestamp:
                    write_to_db = False

                if match_id == 0:
                    write_to_db = False

                player = match_data.get('player', {})

                # The last event will be stored in Datastore if it contains information about players.
                if write_to_db and isinstance(player, dict):
                    # Only live matches data contain players team names.
                    # Retrieve the match_id live data.
                    live_attributes_list = [lm.get('attributes')
                                            for lm in live_matches
                                            if lm.get('match_id') == match_id
                                            and lm.get('attributes') is not None][:1]

                    live_attributes = live_attributes_list[0] if live_attributes_list else {}

                    for gsi_team_name, live_team_name in team_names.items():
                        live_team_name_value = live_attributes.get(live_team_name)

                        if live_team_name_value:
                            # If the player's team is found in matches live data
                            # it will be appended as a new attribute for the player.
                            team_data = player.get(gsi_team_name)
                            if isinstance(team_data, dict):
                                for player_name, player_data in team_data.items():
                                    player_data['team_name'] = live_team_name_value
                        else:
                            # If there is no information about teams for the match_id in the Google Datastore
                            # then additional task to retrieve the live matches data will be triggered.
                            live_matches_refresh = True

                    # Aggregate all of the event's data and return it to be stored in Datastore in the next task.
                    event_data = {'match_id': match_id,
                                  'timestamp': timestamp,
                                  'clock_time': clock_time,
                                  'game_time': game_time,
                                  'token': token,
                                  'match_data': json.dumps(match_data)}

                    yield event_data

        if live_matches_refresh:
            # Send a signal (message) that live matched data have to be refreshed (pulled from Steam API).
            yield beam.pvalue.TaggedOutput('live_matches_refresh', True)


# Downloading the live Dota2 matches(games) data from the Steam REST API
# Storing results as 1 entity in the Google Datastore with entity key = 0.
class DownloadLiveMatches(beam.DoFn):
    def process(self, project_id: str, kind: str):
        import json
        from lib.apis import SteamAPIConnection
        from lib.datastore import EntityModelFactory, DatastoreDb

        live_matches_data = {}
        steam_api_client = SteamAPIConnection().client
        live_matches = steam_api_client.get_live_matches()
        if live_matches:
            live_matches_data['matches'] = json.dumps(live_matches)
            entity = EntityModelFactory.get_model_obj(name=kind, entity=live_matches_data)
            DatastoreDb(project_id=project_id, kind=kind).save_entities([entity])

            yield True


# refresh_rate
def run(*args, **kwargs):
    # service account credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google-cloud-pubsub-publisher-credentials.json"

    project_id = '<GOOGLE-CLOUD-PROJECT-NAME>'
    kind_newdata = 'newdata'
    kind_gsi_event = 'gsi-events'
    kind_live_matches = 'live-matches'

    refresh_rate = kwargs.get('refresh_rate')
    if refresh_rate is None:
        refresh_rate = 0

    window_frame = 5 if refresh_rate <= 0 else refresh_rate

    options = PipelineOptions(None)

    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DataflowRunner'
    standard_options.streaming = True

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.job_name = 'dota2-gsi-events-streamer'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.staging_location = f'gs://{project_id}/dota2-gsi-streaming/binaries'
    google_cloud_options.temp_location = f'gs://{project_id}/dota2-gsi-streaming/tmp'

    worker_options = options.view_as(WorkerOptions)
    worker_options.num_workers = 1
    worker_options.max_num_workers = 5
    worker_options.network = '<GOOGLE-CLOUD-NETWORK-NAME>'
    worker_options.machine_type = 'n1-standard-2'
    worker_options.subnetwork = 'regions/us-central1/subnetworks/us-central'

    # JOB parameters
    pubsub_subscription_name = f'projects/{project_id}/subscriptions/dota2-gsi-queue-subscription'

    # Converting a context to the appropriate datastore entity and saving the entity to Google Datastore
    def save_datastore_entity(kind: str, context):
        from lib.datastore import EntityModelFactory, DatastoreDb
        entity = EntityModelFactory.get_model_obj(name=kind, entity=context)
        DatastoreDb(project_id=project_id, kind=kind).save_entities([entity])

    with beam.Pipeline(options=options) as p:
        # Reading a window of messages
        window_frame = (
            p
            | 'Read' >> beam.io.ReadFromPubSub(subscription=pubsub_subscription_name).with_input_types(str)
            | 'Window' >> beam.WindowInto(window.FixedWindows(window_frame))
        )

        # Extracting events from messages
        events = (
            window_frame
            | 'Validate' >> beam.ParDo(Validate(), kind=kind_newdata)
        )

        # Processing events
        event_data, live_matches_refresh = (
            events
            | 'Parse' >> beam.ParDo(Parse())
            | 'Aggregate by match_id' >> GroupByKey()
            | 'Enrich last event' >> beam.ParDo(EnrichLastEvent(),
                                                project_id=project_id,
                                                kind_live_matches=kind_live_matches,
                                                kind_gsi_event=kind_gsi_event)
            .with_outputs('live_matches_refresh', main='event_data')
        )

        # Writing the last event
        (
            event_data
            | 'Write new event' >> beam.Map(lambda d: save_datastore_entity(kind_gsi_event, d))
        )

        # Loading and storing live matches data
        (
            live_matches_refresh
            | 'Download live teams' >> beam.ParDo(DownloadLiveMatches(),
                                                  project_id=project_id,
                                                  kind=kind_live_matches)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run(sys.argv[1:])
