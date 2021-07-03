import os
import requests
from config import timeout_default
from logsys import LogManager
from all_types import RequestResult
from wrappers import decode_to_utf_8


def request_get(url: str, **kwargs):
    timeout = kwargs.get('timeout', timeout_default)
    auth = kwargs.get('auth', None)
    to_file_path = kwargs.get('file_path', '')
    as_stream = bool(to_file_path)

    response_json = {}
    err_message = ''
    throw_error = True

    logman = LogManager().client

    try:
        if auth:
            response = requests.get(url, timeout=timeout, auth=auth, stream=as_stream)
        else:
            response = requests.get(url, timeout=timeout, stream=as_stream)

        status_code = response.status_code

        if status_code == 200:
            if as_stream:
                empty_input_file = True
                dir_name = os.path.dirname(to_file_path)

                if not os.path.exists(dir_name):
                    os.mkdir(dir_name)

                f = open(to_file_path, 'wb')
                for chunk in response.iter_content(chunk_size=512 * 1024):
                    decode_res, chunk_str = decode_to_utf_8(chunk)
                    correct_chunk = (chunk_str != 'null' and chunk_str != '{"detail":"Not Found"}') \
                        if decode_res \
                        else True

                    if chunk and correct_chunk:
                        f.write(chunk)
                        empty_input_file = False
                f.close()
                response_json = {'downloaded': not empty_input_file}
            else:
                response_json = response.json() if status_code == 200 else {}
    except requests.exceptions.Timeout:
        status_code = 408
        throw_error = False
        err_message = logman.error('requests_408', timeout, url)
    except requests.exceptions.RequestException as ex:
        status_code = 500
        throw_error = False
        err_message = logman.error('requests_500', url, ex)
    except ValueError as ex:
        status_code = 500
        throw_error = False
        err_message = logman.error('requests_500', url, ex)

    if status_code != 200 and throw_error:
        err_message = logman.error('requests_not_200', status_code, url, response.reason)

    return RequestResult(status_code=status_code, response_json=response_json, err_message=err_message)
