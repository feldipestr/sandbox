from datetime import datetime
import json
import re
import hashlib


def get_type_str(val):
    try:
        type_str = type(val).__name__
    except AttributeError:
        type_str = 'unknown type name'

    return type_str


def hash_if_special_characters(str_value: str):
    result = str_value
    p = re.compile('^[a-zA-Z0-9]*$')
    if not p.match(str_value):
        h = hashlib.md5()
        h.update(str_value.encode())
        result = h.hexdigest()
    return result


def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.__str__()


def json_dump(dumping_dict):
    result_dump = ''
    try:
        result_dump = json.dumps(dumping_dict, default=json_serializer)
    except ValueError:
        pass
    return result_dump


def decode_to_utf_8(val):
    try:
        decoded_val = val.decode('utf-8')
        res = True
    except UnicodeDecodeError:
        decoded_val = ''
        res = False

    return res, decoded_val
