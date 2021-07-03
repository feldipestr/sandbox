from os.path import expanduser
from cryptography.fernet import Fernet, InvalidToken


def decr(encrypted_val: str) -> str:
    home = expanduser("~")
    with open(f'{home}/path/key', 'rb') as f:
        key = f.read()
        f = Fernet(key)
        try:
            decrypted = f.decrypt(encrypted_val.encode()).decode('utf-8')
        except InvalidToken:
            decrypted = encrypted_val
    return decrypted


def decrypt_attribute(attributes_collection, attribute_name):
    if attribute_name in attributes_collection:
        attributes_collection[attribute_name] = decr(attributes_collection[attribute_name])
