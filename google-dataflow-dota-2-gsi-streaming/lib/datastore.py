import abc
from google.cloud import datastore
from google.cloud.datastore.entity import Entity


# Abstract class for different Google Datastore entity records
class AbstractDatastoreEntityModel(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def get_entity_key(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def get_not_indexing_attrs(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def get_attributes(self, *args, **kwargs):
        pass


class LiveMatch(AbstractDatastoreEntityModel):
    def __init__(self, **kwargs):
        entity = kwargs.get('entity')
        src = entity if isinstance(entity, dict) else kwargs
        self.matches = src.get('matches', '')

    @property
    def matches(self):
        return self.__matches

    @matches.setter
    def matches(self, val):
        self.__matches = val

    # only one live match entity is supposed to be saved in Google Datastore and it has key='0'
    def get_entity_key(self):
        return '0'

    def get_not_indexing_attrs(self):
        return ['matches']

    def get_attributes(self):
        return {n.replace(f'_{self.__class__.__name__}__', ''): str(v) if type(v) == str else v
                for n, v in self.__dict__.items()}


class GsiEvent(AbstractDatastoreEntityModel):
    def __init__(self, **kwargs):
        entity = kwargs.get('entity')
        src = entity if isinstance(entity, dict) else kwargs
        self.match_id = src.get('match_id', 0)
        self.timestamp = src.get('timestamp', 0)
        self.clock_time = src.get('clock_time', 0)
        self.token = src.get('token', '')
        self.match_data = src.get('match_data', '')

    @property
    def match_id(self):
        return self.__match_id

    @match_id.setter
    def match_id(self, val):
        self.__match_id = val

    @property
    def timestamp(self):
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, val):
        self.__timestamp = val

    @property
    def clock_time(self):
        return self.__clock_time

    @clock_time.setter
    def clock_time(self, val):
        self.__clock_time = val

    @property
    def token(self):
        return self.__token

    @token.setter
    def token(self, val):
        self.__token = val

    @property
    def match_data(self):
        return self.__match_data

    @match_data.setter
    def match_data(self, val):
        self.__match_data = val

    # only one gsi event (last one) entity per spactator (token) is supposed to be saved in Google Datastore
    def get_entity_key(self):
        return str(self.token)

    def get_not_indexing_attrs(self):
        return ['timestamp', 'clock_time', 'match_data']

    def get_attributes(self):
        return {n.replace(f'_{self.__class__.__name__}__', ''): str(v) if type(v) == str else v
                for n, v in self.__dict__.items()}

# factory for Google Datastore entity instances creation
class EntityModelFactory(object):
    __entity_models = {
        'live-matches': LiveMatch,
        'gsi-events': GsiEvent,
    }

    @staticmethod
    def get_model_obj(name, *args, **kwargs) -> AbstractDatastoreEntityModel:
        model_class = EntityModelFactory.__entity_models.get(name.lower(), None)

        if model_class:
            return model_class(*args, **kwargs)
        else:
            print(f'No factory for {name}!')

        return None


# Google Datastore client
class DatastoreDb(object):
    def __init__(self, project_id: str = '', kind: str = ''):
        self.project_id = project_id
        self.kind = kind

    @staticmethod
    def list_chunks(l: list, n: int) -> list:
        for i in range(0, len(l), n):
            yield l[i:i + n]

    # saving a list of entities splitting it on chunks of 25 entities (Datastore limit)
    def save_entities(self, obj_list: list) -> bool:
        res = False

        if self.project_id and self.kind:
            entities = []
            client = datastore.Client(project=self.project_id)
            for obj in obj_list:
                key_value = obj.get_entity_key()
                exclude_from_indexes = obj.get_not_indexing_attrs()
                key = client.key(self.kind, key_value)
                new_entity = datastore.Entity(key=key, exclude_from_indexes=exclude_from_indexes)
                entity_attributes = obj.get_attributes()
                new_entity.update(entity_attributes)
                entities.append(new_entity)

            try:
                for chunk in DatastoreDb.list_chunks(entities, 25):
                    with client.transaction():
                        client.put_multi(chunk)
                res = True
            except ValueError:
                res = False

        return res

    def query_entity(self, key_value: (int, str), kind: str = '') -> Entity:
        if not kind:
            kind = self.kind

        assert kind

        client = datastore.Client(project=self.project_id)
        key = client.key(kind, key_value)
        entity = client.get(key)

        return entity